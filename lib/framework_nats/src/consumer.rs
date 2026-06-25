use std::any::type_name;
use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

use async_nats::HeaderValue;
use async_nats::jetstream;
use async_nats::jetstream::consumer::AckPolicy;
use async_nats::jetstream::consumer::pull;
use chrono::DateTime;
use chrono::SecondsFormat;
use chrono::Utc;
use framework::console;
use framework::context;
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

// decoded message handed to call-site handlers; the framework works with the raw jetstream message.
pub struct Message<T> {
    pub subject: String,
    pub payload: T,
}

#[derive(Clone, Copy)]
pub struct ConsumerConfig {
    // max in-flight message handlers for MessageConsumer (semaphore size); ignored by BatchConsumer.
    pub max_concurrency: usize,
    pub batch_max_messages: usize,
    pub batch_max_wait: Duration,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self { max_concurrency: 100, batch_max_messages: 1000, batch_max_wait: Duration::from_secs(1) }
    }
}

// shared across every consumer in the process; reports overall in-flight handlers.
static MESSAGE_COUNTER: OnceLock<Counter> = OnceLock::new();

pub fn consumer_metrics() -> impl Fn(&mut Metrics) {
    MESSAGE_COUNTER.set(Counter::new()).unwrap_or_else(|_| panic!("consumer_metrics can only be called once"));
    |metrics| {
        if let Some(counter) = MESSAGE_COUNTER.get() {
            metrics.stats.push(("active_message_handlers", counter.max() as u64));
        }
    }
}

type MessageHandler<S> = Box<dyn Fn(jetstream::Message, S) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

// one durable pull consumer over a whole stream that may carry different subject types. each
// subject is registered with its own handler; messages are pulled continuously via sequence(),
// dispatched by subject, and processed in their own task (bounded by a semaphore) that acks itself.
pub struct MessageConsumer<S> {
    url: String,
    stream: &'static str,
    durable: &'static str,
    handlers: HashMap<&'static str, MessageHandler<S>>,
    config: ConsumerConfig,
}

impl<S> MessageConsumer<S>
where
    S: Clone + Send + Sync + 'static,
{
    pub fn new(url: String, stream: &'static str, durable: &'static str, config: ConsumerConfig) -> Self {
        Self { url, stream, durable, handlers: HashMap::new(), config }
    }

    pub fn add_handler<H, Fut, M>(&mut self, subject: &Subject<M>, handler: H)
    where
        H: Fn(S, Message<M>) -> Fut + Copy + Send + Sync + 'static,
        Fut: Future<Output = Result<(), Exception>> + Send + 'static,
        M: DeserializeOwned + Send + 'static,
    {
        let wrapper: MessageHandler<S> = Box::new(move |raw, state| Box::pin(handle_message(raw, handler, state)));
        self.handlers.insert(subject.name, wrapper);
    }

    pub async fn start(self, state: S, shutdown_signal: CancellationToken) {
        let Self { url, stream, durable, handlers, config } = self;

        let connection = async_nats::connect(url).await.expect("failed to connect nats"); // fail fast on startup
        let context = jetstream::new(connection);
        let stream_handle = context
            .get_stream(stream)
            .await
            .unwrap_or_else(|e| panic!("failed to get stream, stream={stream}, error={e:?}")); // fail fast on startup

        let subjects: Vec<String> = handlers.keys().map(|subject| (*subject).to_owned()).collect();
        console!("nats consumer started, stream={stream}, subjects={subjects:?}");

        let consumer = stream_handle
            .get_or_create_consumer(
                durable,
                pull::Config {
                    durable_name: Some(durable.to_owned()),
                    ack_policy: AckPolicy::Explicit,
                    ack_wait: Duration::from_mins(30),
                    filter_subjects: subjects,
                    ..Default::default()
                },
            )
            .await
            .expect("failed to create consumer"); // fail fast on startup

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
                                console!("ERROR failed to read message, error={e:?}");
                                continue;
                            }
                        };

                        let subject = raw.subject.as_str();
                        let Some(handler) = handlers.get(subject) else {
                            console!("WARN no handler registered, subject={subject}");
                            if let Err(e) = raw.ack().await {
                                console!("ERROR failed to ack message, error={e:?}");
                            }
                            continue;
                        };
                        let permit = Arc::clone(&semaphore).acquire_owned().await.expect("semaphore should not close");
                        let name = format!("message:{subject}");
                        let task = handler(raw, state.clone());
                        executor.spawn(name, async move {
                            let _permit = permit; // held until the handler (and its ack) completes
                            task.await;
                        });
                    }
                }
                Err(e) => {
                    console!("ERROR failed to fetch messages, error={e:?}");
                    time::sleep(Duration::from_secs(5)).await;
                }
            }

            if shutdown_signal.is_cancelled() {
                break;
            }
        }

        // graceful shutdown: stop pulling, then wait for every in-flight handler (and its ack) to finish,
        // aborting any that overrun the drain timeout (their messages are redelivered later).
        if let Some(aborted) = executor.shutdown(Duration::from_secs(30)).await {
            console!("WARN message aborted, messages={aborted:?}");
        }

        console!(
            "nats consumer stopped, name={durable}, stream={stream}, subjects={:?}",
            consumer.cached_info().config.filter_subjects
        );
    }
}

async fn handle_message<H, S, M, Fut>(raw: jetstream::Message, handler: H, state: S)
where
    H: Fn(S, Message<M>) -> Fut,
    Fut: Future<Output = Result<(), Exception>>,
    M: DeserializeOwned,
{
    let _counter = MESSAGE_COUNTER.get().map(Counter::increase);
    let ref_id = header(&raw, REF_ID).map(|id| vec![id.to_owned()]);
    let _result = log::start_action("message", ref_id, async {
        let subject = raw.subject.to_string();
        context!(subject = &subject, fn = type_name::<H>());
        log!("[message] payload={}", String::from_utf8_lossy(&raw.payload));
        if let Some(timestamp) = timestamp(&raw) {
            log!("[message] timestamp={:?}", timestamp.to_rfc3339_opts(SecondsFormat::Millis, true));
            let lag = Utc::now() - timestamp;
            stats!(nats_consumer_lag = lag.num_nanoseconds().unwrap_or_default());
        }
        if let Some(client) = header(&raw, CLIENT) {
            context!(client = client);
        }
        stats!(nats_read_entries = 1, nats_read_bytes = raw.payload.len());
        let result = match from_json::<M>(&String::from_utf8_lossy(&raw.payload)) {
            Ok(payload) => handler(state, Message { subject, payload }).await,
            Err(e) => Err(exception!("failed to decode message", code = "NATS_INVALID_MESSAGE", source = e)),
        };
        // always ack (at-most-once): log ack failures but never block redelivery on handler errors.
        if let Err(e) = raw.ack().await {
            log!(
                exception = exception!(
                    "failed to ack message",
                    code = "NATS_ACK_FAILED",
                    source = Exception::from_dyn(e.as_ref())
                )
            );
        }
        result
    })
    .await;
}

// one durable pull consumer for a single subject/type. a single task pulls a batch, hands the
// whole batch to one handler, then acks the latest message; AckPolicy::All makes that single ack
// cover the entire batch.
pub struct BatchConsumer<S, M, H> {
    url: String,
    stream: &'static str,
    durable: &'static str,
    subject: &'static str,
    handler: H,
    config: ConsumerConfig,
    _marker: PhantomData<fn(S, M)>,
}

impl<S, M, H, Fut> BatchConsumer<S, M, H>
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
        config: ConsumerConfig,
    ) -> Self {
        Self { url, stream, durable, subject: subject.name, handler, config, _marker: PhantomData }
    }

    pub async fn start(self, state: S, shutdown_signal: CancellationToken) {
        let Self { url, stream, durable, subject, handler, config, .. } = self;

        let connection = async_nats::connect(url).await.expect("failed to connect nats"); // fail fast on startup
        let context = jetstream::new(connection);
        let stream_handle = context
            .get_stream(stream)
            .await
            .unwrap_or_else(|e| panic!("failed to get stream, stream={stream}, error={e:?}")); // fail fast on startup

        console!("nats batch consumer started, stream={stream}, subject={subject}");

        let consumer = stream_handle
            .get_or_create_consumer(
                durable,
                pull::Config {
                    durable_name: Some(durable.to_owned()),
                    filter_subject: subject.to_owned(),
                    ack_policy: AckPolicy::All,
                    ack_wait: Duration::from_mins(30),
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
                                console!("ERROR failed to read message, subject={subject}, error={e:?}");
                            }
                        }
                    }
                    if !raw_messages.is_empty() {
                        handle_batch(subject, &raw_messages, &handler, state.clone()).await;
                    }
                }
                Err(e) => {
                    console!("ERROR failed to fetch messages, subject={subject}, error={e:?}");
                    time::sleep(Duration::from_secs(5)).await;
                }
            }

            // single task; a batch always completes before this check, so shutdown is inherently graceful.
            if shutdown_signal.is_cancelled() {
                break;
            }
        }

        console!("nats batch consumer stopped, name={durable}, stream={stream}, subject={subject}");
    }
}

async fn handle_batch<H, S, M, Fut>(subject: &'static str, raw_messages: &[jetstream::Message], handler: &H, state: S)
where
    H: Fn(S, Vec<Message<M>>) -> Fut,
    Fut: Future<Output = Result<(), Exception>>,
    M: DeserializeOwned,
{
    let _counter = MESSAGE_COUNTER.get().map(Counter::increase);
    let ref_id: Option<Vec<String>> = raw_messages.iter().map(|raw| header(raw, REF_ID).map(str::to_owned)).collect();
    let _result = log::start_action("message", ref_id, async move {
        context!(subject = subject, fn = type_name::<H>());
        if let Some(client) =
            raw_messages.iter().map(|raw| header(raw, CLIENT).map(str::to_owned)).collect::<Option<Vec<String>>>()
        {
            context!(client = client);
        }
        let mut bytes = 0;
        let mut messages: Vec<Message<M>> = Vec::with_capacity(raw_messages.len());
        for raw in raw_messages {
            bytes += raw.payload.len();
            let payload = String::from_utf8_lossy(&raw.payload);
            log!("[message] payload={}", &payload);
            match from_json::<M>(&payload) {
                Ok(payload) => messages.push(Message { payload, subject: raw.subject.to_string() }),
                Err(e) => {
                    log!(exception = exception!("failed to decode message", code = "NATS_INVALID_MESSAGE", source = e));
                }
            }
        }
        stats!(nats_read_messages = messages.len(), nats_read_bytes = bytes);
        if let Some(timestamp) = raw_messages.first().and_then(timestamp) {
            log!("[message] timestamp={:?}", timestamp.to_rfc3339_opts(SecondsFormat::Millis, true));
            let lag = Utc::now() - timestamp;
            stats!(nats_consumer_lag = lag.num_nanoseconds().unwrap_or_default());
        }
        if let Some(clients) =
            raw_messages.iter().map(|raw| header(raw, CLIENT).map(str::to_owned)).collect::<Option<Vec<String>>>()
        {
            context!(client = clients);
        }

        let result = handler(state, messages).await;
        // ack the latest only; AckPolicy::All acks the whole batch. always ack regardless of result.
        if let Some(raw) = raw_messages.last()
            && let Err(e) = raw.ack().await
        {
            log!(
                exception = exception!(
                    "failed to ack message",
                    code = "NATS_ACK_FAILED",
                    source = Exception::from_dyn(e.as_ref())
                )
            );
        }
        result
    })
    .await;
}

// ref_id and client headers are set and consumed by the framework only.
fn header<'a>(raw: &'a jetstream::Message, name: &str) -> Option<&'a str> {
    raw.headers.as_ref()?.get(name).map(HeaderValue::as_str)
}

// info() carries the server-side publish time; best-effort, used only for lag.
fn timestamp(raw: &jetstream::Message) -> Option<DateTime<Utc>> {
    let info = raw.info().ok()?;
    DateTime::from_timestamp(info.published.unix_timestamp(), info.published.nanosecond())
}
