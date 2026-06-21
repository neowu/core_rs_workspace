use std::any::type_name;
use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

use async_nats::jetstream;
use async_nats::jetstream::consumer::AckPolicy;
use async_nats::jetstream::consumer::pull;
use chrono::DateTime;
use chrono::SecondsFormat;
use chrono::Utc;
use framework::console;
use framework::context;
use framework::error;
use framework::exception;
use framework::exception::Exception;
use framework::json::from_json;
use framework::log;
use framework::log::metrics::Counter;
use framework::log::metrics::Metrics;
use framework::stats;
use framework::task::TaskExecutor;
use futures::StreamExt as _;
use serde::de::DeserializeOwned;
use tokio::sync::Semaphore;
use tokio::time;
use tokio_util::sync::CancellationToken;

use crate::CLIENT;
use crate::REF_ID;
use crate::Subject;

pub struct Message<T: DeserializeOwned> {
    pub payload: String,
    pub headers: HashMap<String, String>,
    pub timestamp: Option<DateTime<Utc>>,
    _marker: PhantomData<T>,
}

impl<T: DeserializeOwned> Message<T> {
    pub fn payload(&self) -> Result<T, Exception> {
        from_json(&self.payload)
    }

    fn from_nats(raw: &jetstream::Message) -> Self {
        let payload = String::from_utf8_lossy(&raw.payload).to_string();

        let mut headers = HashMap::new();
        if let Some(header_map) = &raw.headers {
            for (name, values) in header_map.iter() {
                let Some(value) = values.last() else { continue };
                let name = name.to_string();
                headers.insert(name, value.as_str().to_owned());
            }
        }

        // info() carries the server-side publish time; best-effort, used only for lag.
        let timestamp = raw
            .info()
            .ok()
            .and_then(|info| DateTime::from_timestamp(info.published.unix_timestamp(), info.published.nanosecond()));

        Message { payload, headers, timestamp, _marker: PhantomData }
    }
}

#[derive(Clone, Copy)]
pub struct ConsumerConfigV2 {
    // max in-flight message handlers for MessageConsumerV2 (semaphore size); ignored by BatchConsumerV2.
    pub max_concurrency: usize,
    pub batch_max_messages: usize,
    pub batch_max_wait: Duration,
    pub ack_wait: Duration,
    // max delivery attempts before the message is dropped by the server; -1 is unlimited.
    pub max_deliver: i64,
}

impl Default for ConsumerConfigV2 {
    fn default() -> Self {
        Self {
            max_concurrency: 100,
            batch_max_messages: 1000,
            batch_max_wait: Duration::from_secs(1),
            ack_wait: Duration::from_secs(30),
            max_deliver: -1,
        }
    }
}

// how long graceful shutdown waits for in-flight handlers before aborting them.
const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

// shared across every v2 consumer in the process; reports overall in-flight handlers.
static MESSAGE_COUNTER: OnceLock<Counter> = OnceLock::new();

pub fn consumer_metrics() -> impl Fn(&mut Metrics) {
    MESSAGE_COUNTER.set(Counter::new()).unwrap_or_else(|_| panic!("consumer_metrics can only be called once"));
    |metrics| {
        if let Some(counter) = MESSAGE_COUNTER.get() {
            metrics.stats.push(("active_message_handlers", counter.max() as u64));
        }
    }
}

type SingleHandler<S> = Arc<dyn Fn(S, jetstream::Message) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

// one durable pull consumer over a whole stream that may carry different subject types. each
// subject is registered with its own handler; messages are pulled continuously via sequence(),
// dispatched by subject, and processed in their own task (bounded by a semaphore) that acks itself.
pub struct MessageConsumerV2<S> {
    url: String,
    stream: &'static str,
    durable: &'static str,
    handlers: HashMap<&'static str, SingleHandler<S>>,
    config: ConsumerConfigV2,
}

impl<S> MessageConsumerV2<S>
where
    S: Clone + Send + Sync + 'static,
{
    pub fn new(url: String, stream: &'static str, durable: &'static str, config: &ConsumerConfigV2) -> Self {
        Self { url, stream, durable, handlers: HashMap::new(), config: *config }
    }

    pub fn add_handler<H, Fut, M>(&mut self, subject: &Subject<M>, handler: H)
    where
        H: Fn(S, Message<M>) -> Fut + Copy + Send + Sync + 'static,
        Fut: Future<Output = Result<(), Exception>> + Send + 'static,
        M: DeserializeOwned + Send + 'static,
    {
        let subject = subject.name;
        let wrapper: SingleHandler<S> = Arc::new(move |state: S, raw: jetstream::Message| {
            Box::pin(handle_message(subject, raw, handler, state)) as Pin<Box<dyn Future<Output = ()> + Send>>
        });
        self.handlers.insert(subject, wrapper);
    }

    pub async fn start(self, state: S, shutdown_signal: CancellationToken) {
        let Self { url, stream, durable, handlers, config } = self;

        let connection = async_nats::connect(url).await.expect("failed to connect nats"); // fail fast on startup
        let context = jetstream::new(connection);
        let stream_handle =
            context.get_stream(stream).await.unwrap_or_else(|e| panic!("failed to get stream, error={e}")); // fail fast on startup

        let subjects: Vec<String> = handlers.keys().map(|subject| (*subject).to_owned()).collect();
        console!("nats consumer v2 started, stream={stream}, subjects={subjects:?}");

        let consumer = stream_handle
            .get_or_create_consumer(
                durable,
                pull::Config {
                    durable_name: Some(durable.to_owned()),
                    filter_subjects: subjects.clone(),
                    ack_policy: AckPolicy::Explicit,
                    ack_wait: config.ack_wait,
                    max_deliver: config.max_deliver,
                    ..Default::default()
                },
            )
            .await
            .expect("failed to create consumer"); // fail fast on startup

        let handlers = Arc::new(handlers);
        let semaphore = Arc::new(Semaphore::new(config.max_concurrency));
        let mut executor = TaskExecutor::default();

        // re-issue a bounded batch pull each round; it expires after batch_max_wait, so the
        // shutdown check runs at least that often without needing to race the pull.
        loop {
            match consumer
                .batch()
                .max_messages(config.batch_max_messages)
                .expires(config.batch_max_wait)
                .messages()
                .await
            {
                Ok(mut batch) => {
                    while let Some(message) = batch.next().await {
                        let raw = match message {
                            Ok(raw) => raw,
                            Err(e) => {
                                console!(
                                    "ERROR {}",
                                    exception!("failed to read message", source = Exception::from_dyn(e.as_ref()))
                                );
                                continue;
                            }
                        };

                        let Some(handler) = handlers.get(raw.subject.as_str()).cloned() else {
                            console!("WARN no handler registered, subject={}", raw.subject.as_str());
                            if let Err(e) = raw.ack().await {
                                error!(
                                    error_code = "NATS_ACK_FAILED",
                                    "{}",
                                    exception!("failed to ack message", source = Exception::from_dyn(e.as_ref()))
                                );
                            }
                            continue;
                        };

                        let permit = Arc::clone(&semaphore).acquire_owned().await.expect("semaphore closed");
                        let state = state.clone();
                        let name = format!("message:{}", raw.subject.as_str());
                        executor.spawn(name, async move {
                            let _permit = permit;
                            let _counter = MESSAGE_COUNTER.get().map(Counter::increase);
                            handler(state, raw).await;
                        });
                    }
                }
                Err(e) => {
                    console!("ERROR {}", exception!("failed to fetch messages", source = e));
                    time::sleep(Duration::from_secs(5)).await;
                }
            }

            if shutdown_signal.is_cancelled() {
                break;
            }
        }

        // graceful shutdown: stop pulling, then wait for every in-flight handler (and its ack) to finish,
        // aborting any that overrun the drain timeout (their messages are redelivered later).
        if let Some(aborted) = executor.shutdown(SHUTDOWN_DRAIN_TIMEOUT).await {
            console!("WARN message aborted, messages={aborted:?}");
        }
        console!("nats consumer v2 stopped, stream={stream}, subjects={subjects:?}");
    }
}

async fn handle_message<H, S, M, Fut>(subject: &'static str, raw: jetstream::Message, handler: H, state: S)
where
    H: Fn(S, Message<M>) -> Fut,
    Fut: Future<Output = Result<(), Exception>>,
    M: DeserializeOwned,
{
    let message = Message::<M>::from_nats(&raw);
    let ref_id = message.headers.get(REF_ID).map(|id| vec![id.to_owned()]);
    let _result = log::start_action("message", ref_id, async {
        context!(subject = subject, fn = type_name::<H>());
        log!("[message] timestamp={:?}", message.timestamp.map(|t| t.to_rfc3339_opts(SecondsFormat::Millis, true)));
        log!("[message] payload={}", message.payload);
        for (key, value) in &message.headers {
            log!("[header] {key}={value}");
        }
        if let Some(client) = message.headers.get(CLIENT) {
            context!(client = client);
        }
        stats!(nats_read_entries = 1, nats_read_bytes = message.payload.len());
        if let Some(timestamp) = message.timestamp {
            let lag = Utc::now() - timestamp;
            log!("lag={lag}");
        }
        let result = handler(state, message).await;
        // always ack (at-most-once): log ack failures but never block redelivery on handler errors.
        if let Err(e) = raw.ack().await {
            error!(
                error_code = "NATS_ACK_FAILED",
                "{}",
                exception!("failed to ack message", source = Exception::from_dyn(e.as_ref()))
            );
        }
        result
    })
    .await;
}

// one durable pull consumer for a single subject/type. a single task pulls a batch, hands the
// whole batch to one handler, then acks the latest message; AckPolicy::All makes that single ack
// cover the entire batch.
pub struct BatchConsumerV2<S, M, H> {
    url: String,
    stream: &'static str,
    durable: &'static str,
    subject: &'static str,
    handler: H,
    config: ConsumerConfigV2,
    _marker: PhantomData<fn(S, M)>,
}

impl<S, M, H, Fut> BatchConsumerV2<S, M, H>
where
    S: Clone + Send + Sync + 'static,
    M: DeserializeOwned + Send + 'static,
    H: Fn(S, Vec<Message<M>>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), Exception>> + Send + 'static,
{
    pub fn new(
        url: String,
        stream: &'static str,
        durable: &'static str,
        subject: &Subject<M>,
        handler: H,
        config: &ConsumerConfigV2,
    ) -> Self {
        Self { url, stream, durable, subject: subject.name, handler, config: *config, _marker: PhantomData }
    }

    pub async fn start(self, state: S, shutdown_signal: CancellationToken) {
        let Self { url, stream, durable, subject, handler, config, .. } = self;

        let connection = async_nats::connect(url).await.expect("failed to connect nats"); // fail fast on startup
        let context = jetstream::new(connection);
        let stream_handle =
            context.get_stream(stream).await.unwrap_or_else(|e| panic!("failed to get stream, error={e}")); // fail fast on startup

        console!("nats batch consumer v2 started, stream={stream}, subject={subject}");

        let consumer = stream_handle
            .get_or_create_consumer(
                durable,
                pull::Config {
                    durable_name: Some(durable.to_owned()),
                    filter_subject: subject.to_owned(),
                    ack_policy: AckPolicy::All,
                    ack_wait: config.ack_wait,
                    max_deliver: config.max_deliver,
                    ..Default::default()
                },
            )
            .await
            .expect("failed to create consumer"); // fail fast on startup

        loop {
            match consumer
                .batch()
                .max_messages(config.batch_max_messages)
                .expires(config.batch_max_wait)
                .messages()
                .await
            {
                Ok(mut batch) => {
                    let mut raw_messages = Vec::new();
                    while let Some(result) = batch.next().await {
                        match result {
                            Ok(message) => raw_messages.push(message),
                            Err(e) => {
                                console!(
                                    "ERROR {}",
                                    exception!(
                                        format!("failed to read message, subject={subject}"),
                                        source = Exception::from_dyn(e.as_ref())
                                    )
                                );
                            }
                        }
                    }
                    if !raw_messages.is_empty() {
                        handle_batch(subject, &raw_messages, &handler, state.clone()).await;
                    }
                }
                Err(e) => {
                    console!(
                        "ERROR {}",
                        exception!(
                            format!("failed to fetch messages, subject={subject}"),
                            source = Exception::from_dyn(&e)
                        )
                    );
                    time::sleep(Duration::from_secs(5)).await;
                }
            }

            // single task; a batch always completes before this check, so shutdown is inherently graceful.
            if shutdown_signal.is_cancelled() {
                break;
            }
        }

        console!("nats batch consumer v2 stopped, stream={stream}, subject={subject}");
    }
}

async fn handle_batch<H, S, M, Fut>(subject: &'static str, raw_messages: &[jetstream::Message], handler: &H, state: S)
where
    H: Fn(S, Vec<Message<M>>) -> Fut,
    Fut: Future<Output = Result<(), Exception>>,
    M: DeserializeOwned,
{
    let messages: Vec<Message<M>> = raw_messages.iter().map(Message::from_nats).collect();
    let ref_id: Option<Vec<String>> = messages.iter().map(|m| m.headers.get(REF_ID).map(String::to_owned)).collect();
    let _result = log::start_action("message", ref_id, async move {
        let _counter = MESSAGE_COUNTER.get().map(Counter::increase);
        context!(subject = subject, fn = type_name::<H>());
        if let Some(client) =
            messages.iter().map(|m| m.headers.get(CLIENT).map(String::to_owned)).collect::<Option<Vec<String>>>()
        {
            context!(client = client);
        }
        let mut bytes = 0;
        for message in &messages {
            log!("[message] payload={}", message.payload);
            bytes += message.payload.len();
        }
        stats!(nats_read_messages = messages.len(), nats_read_bytes = bytes);
        if let Some(timestamp) = messages.first().and_then(|message| message.timestamp) {
            let lag = Utc::now() - timestamp;
            log!("lag={lag}");
        }
        let result = handler(state, messages).await;
        // ack the latest only; AckPolicy::All acks the whole batch. always ack regardless of result.
        if let Some(raw) = raw_messages.last()
            && let Err(e) = raw.ack().await
        {
            error!(
                error_code = "NATS_ACK_FAILED",
                "{}",
                exception!(
                    format!("failed to ack message, subject={subject}"),
                    source = Exception::from_dyn(e.as_ref())
                )
            );
        }
        result
    })
    .await;
}
