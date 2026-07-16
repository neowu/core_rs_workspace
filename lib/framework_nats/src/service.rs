use std::any::TypeId;
use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::mem::transmute_copy;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_nats::Client;
use async_nats::HeaderMap;
use async_nats::HeaderValue;
use async_nats::Message;
use async_nats::RequestErrorKind;
use framework::api::ErrorResponse;
use framework::console;
use framework::context;
use framework::exception;
use framework::exception::Exception;
use framework::json::from_json;
use framework::json::to_json;
use framework::log;
use framework::log::current_action_id;
use framework::span;
use framework::stats;
use framework::string::intern;
use framework::task::TaskExecutor;
use futures::StreamExt as _;
use futures::stream::select_all;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::CLIENT;
use crate::ERROR;
use crate::REF_ID;

#[derive(Clone, Copy)]
pub struct ServiceConfig {
    // max in-flight request handlers (semaphore size)
    pub max_concurrency: usize,
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self { max_concurrency: 100 }
    }
}

type RequestHandler = Box<dyn Fn(Client, Message) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

// core nats request/reply service. each subject is registered with its own handler; subscriptions
// use the subject as queue group so multiple service instances load balance. requests are processed
// in their own task (bounded by a semaphore) that publishes the reply itself.
pub struct Service {
    nats_client: Client,
    handlers: HashMap<&'static str, RequestHandler>,
    config: ServiceConfig,
}

impl Service {
    pub fn new(nats_client: Client) -> Self {
        Self::with_config(nats_client, ServiceConfig::default())
    }

    pub fn with_config(nats_client: Client, config: ServiceConfig) -> Self {
        Self { nats_client, handlers: HashMap::new(), config }
    }

    pub fn add_handler<H, Fut, Req, Res>(&mut self, subject: &'static str, handler: H)
    where
        H: Fn(Req) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Res, Exception>> + Send + 'static,
        Req: DeserializeOwned + Send + 'static,
        Res: Serialize + Debug + Send + 'static,
    {
        let handler = Arc::new(handler);
        let wrapper: RequestHandler =
            Box::new(move |client, message| Box::pin(handle_request(client, message, Arc::clone(&handler))));
        self.handlers.insert(subject, wrapper);
    }

    pub async fn start(self, shutdown_signal: CancellationToken) {
        let Self { nats_client, handlers, config } = self;

        let mut subscribers = Vec::with_capacity(handlers.len());
        for subject in handlers.keys() {
            let subscriber = nats_client
                .queue_subscribe(*subject, (*subject).to_owned()) // queue group = subject, multiple instances load balance
                .await
                .unwrap_or_else(|e| panic!("failed to subscribe, subject={subject}, error={e:?}")); // fail fast on startup
            subscribers.push(subscriber);
        }

        let subjects: Vec<&str> = handlers.keys().copied().collect();
        console!("nats service started, subjects={subjects:?}");

        let mut requests = select_all(subscribers);
        let semaphore = Arc::new(Semaphore::new(config.max_concurrency));
        let mut executor = TaskExecutor::default();

        loop {
            tokio::select! {
                () = shutdown_signal.cancelled() => break,
                message = requests.next() => {
                    let Some(message) = message else { break };
                    let Some(handler) = handlers.get(message.subject.as_str()) else {
                        console!("WARN no handler registered, subject={}", message.subject);
                        continue;
                    };
                    let permit = Arc::clone(&semaphore).acquire_owned().await.expect("semaphore should not close");
                    let name = format!("request:{}", message.subject);
                    let task = handler(nats_client.clone(), message);
                    executor.spawn(name, async move {
                        let _permit = permit; // held until the handler (and its reply) completes
                        task.await;
                    });
                }
            }
        }

        // graceful shutdown: unsubscribe so pending requests fail over to other queue group members,
        // then wait for every in-flight handler (and its reply) to finish, aborting any that overrun.
        drop(requests);
        if let Some(aborted) = executor.shutdown(Duration::from_secs(30)).await {
            console!("WARN request aborted, requests={aborted:?}");
        }

        console!("nats service stopped, subjects={subjects:?}");
    }
}

async fn handle_request<H, Fut, Req, Res>(client: Client, message: Message, handler: Arc<H>)
where
    H: Fn(Req) -> Fut,
    Fut: Future<Output = Result<Res, Exception>>,
    Req: DeserializeOwned + 'static,
    Res: Serialize + Debug + 'static,
{
    let ref_id = header(&message, REF_ID).map(|id| vec![id.to_owned()]);
    let _result = log::action("nats", ref_id, async {
        context!(subject = message.subject.as_str());
        if let Some(client_name) = header(&message, CLIENT) {
            context!(client = client_name);
        }
        log!("[request] payload={}", String::from_utf8_lossy(&message.payload));
        stats!(nats_request_messages = 1, nats_request_bytes = message.payload.len());

        let result = match decode::<Req>(&message.payload) {
            Ok(request) => handler(request).await,
            Err(e) => Err(exception!("failed to decode request", code = "NATS_INVALID_MESSAGE", source = e)),
        };

        let Some(reply) = message.reply else {
            return result.map(|_| ());
        };
        match result {
            Ok(response) => {
                let payload = encode(&response)?;
                log!("[reply] payload={payload}");
                client.publish(reply, payload.into()).await?;
                Ok(())
            }
            Err(e) => {
                let body =
                    ErrorResponse { severity: e.severity, code: e.code.map(str::to_owned), message: e.message.clone() };
                let mut headers = HeaderMap::new();
                headers.insert(ERROR, "true");
                client.publish_with_headers(reply, headers, to_json(&body)?.into()).await?;
                Err(e)
            }
        }
    })
    .await;
}

// nats api client, mirrors framework::web::api::ApiClient
pub struct ServiceClient {
    nats_client: Client,
    client: &'static str,
}

impl ServiceClient {
    // client usually be env!("CARGO_BIN_NAME")
    pub const fn new(nats_client: Client, client: &'static str) -> Self {
        Self { nats_client, client }
    }

    pub async fn request<Req, Res>(&self, subject: &'static str, request: &Req) -> Result<Res, Exception>
    where
        Req: Serialize + Debug + 'static,
        Res: DeserializeOwned + 'static,
    {
        let _span = span!("nats");
        let payload = encode(request)?;

        stats!(nats_request_messages = 1, nats_request_bytes = payload.len());

        let mut headers = HeaderMap::new();
        headers.insert(CLIENT, self.client);
        if let Some(ref_id) = current_action_id() {
            headers.insert(REF_ID, ref_id);
        }

        log!("request, subject={subject}, payload={payload}");
        // reply must arrive within the connection level request timeout (async-nats default: 10s),
        // configurable via ConnectOptions::request_timeout
        let reply =
            self.nats_client.request_with_headers(subject, headers, payload.into()).await.map_err(|e| {
                match e.kind() {
                    RequestErrorKind::NoResponders => {
                        exception!(format!("no responders, subject={subject}"), code = "NATS_NO_RESPONDERS")
                    }
                    RequestErrorKind::TimedOut => {
                        exception!(format!("request timed out, subject={subject}"), code = "NATS_TIMEOUT")
                    }
                    RequestErrorKind::InvalidSubject
                    | RequestErrorKind::MaxPayloadExceeded
                    | RequestErrorKind::Other => {
                        exception!(format!("failed to send request, subject={subject}"), source = e)
                    }
                }
            })?;
        parse_reply(subject, &reply)
    }
}

fn parse_reply<Res>(subject: &str, reply: &Message) -> Result<Res, Exception>
where
    Res: DeserializeOwned + 'static,
{
    let payload = String::from_utf8_lossy(&reply.payload);
    log!("reply, payload={payload}");
    if reply.headers.as_ref().is_some_and(|headers| headers.get(ERROR).is_some()) {
        let error: ErrorResponse = from_json(&payload)?;
        if let Some(ref code) = error.code {
            Err(exception!(
                format!("failed to call service, subject={subject}, error={}", error.message),
                severity = error.severity,
                code = intern(code)
            ))
        } else {
            Err(exception!(
                format!("failed to call service, subject={subject}, error={}", error.message),
                severity = error.severity
            ))
        }
    } else if TypeId::of::<Res>() == TypeId::of::<()>() {
        // SAFETY: We've verified Res is () via TypeId, so this transmute is sound.
        Ok(unsafe { transmute_copy(&()) })
    } else {
        from_json(&payload)
    }
}

fn decode<Req>(payload: &[u8]) -> Result<Req, Exception>
where
    Req: DeserializeOwned + 'static,
{
    if TypeId::of::<Req>() == TypeId::of::<()>() {
        // SAFETY: We've verified Req is () via TypeId, so this transmute is sound.
        Ok(unsafe { transmute_copy(&()) })
    } else {
        from_json(&String::from_utf8_lossy(payload))
    }
}

fn encode<T>(value: &T) -> Result<String, Exception>
where
    T: Serialize + Debug + 'static,
{
    if TypeId::of::<T>() == TypeId::of::<()>() { Ok(String::new()) } else { to_json(value) }
}

// ref_id and client headers are set and consumed by the framework only.
fn header<'a>(message: &'a Message, name: &str) -> Option<&'a str> {
    message.headers.as_ref()?.get(name).map(HeaderValue::as_str)
}
