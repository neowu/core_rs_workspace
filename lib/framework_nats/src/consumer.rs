use std::any::type_name;
use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream;
use async_nats::jetstream::consumer::AckPolicy;
use async_nats::jetstream::consumer::PullConsumer;
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
use futures::StreamExt as _;
use serde::de::DeserializeOwned;
use tokio::task::JoinSet;
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

type MessageHandler<S> = Box<dyn Fn(S, Vec<jetstream::Message>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

#[derive(Clone, Copy)]
pub struct ConsumerConfig {
    pub batch_max_messages: usize,
    pub batch_max_wait: Duration,
    pub ack_wait: Duration,
    // max delivery attempts before the message is dropped by the server; -1 is unlimited.
    pub max_deliver: i64,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            batch_max_messages: 1000,
            batch_max_wait: Duration::from_secs(1),
            ack_wait: Duration::from_secs(30),
            max_deliver: -1,
        }
    }
}

pub struct MessageConsumer<S> {
    url: String,
    stream: &'static str,
    durable_prefix: &'static str,
    handlers: Vec<(&'static str, MessageHandler<S>)>,
    config: ConsumerConfig,
    counter: Arc<Counter>,
}

impl<S> MessageConsumer<S>
where
    S: Clone + Send + Sync + 'static,
{
    // durable_prefix usually be env!("CARGO_BIN_NAME"); each subject gets its own durable
    // pull consumer named "{durable_prefix}-{subject}".
    pub fn new(url: String, stream: &'static str, durable_prefix: &'static str, config: &ConsumerConfig) -> Self {
        Self { url, stream, durable_prefix, handlers: Vec::new(), config: *config, counter: Arc::new(Counter::new()) }
    }

    pub fn consumer_metrics(&self) -> impl Fn(&mut Metrics) + use<S> {
        let counter = Arc::clone(&self.counter);
        move |metrics| {
            metrics.stats.push(("active_message_handlers", counter.max() as u64));
        }
    }

    pub fn add_handler<H, Fut, M>(&mut self, subject: &Subject<M>, handler: H)
    where
        H: Fn(S, Message<M>) -> Fut + Copy + Send + Sync + 'static,
        Fut: Future<Output = Result<(), Exception>> + Send + 'static,
        M: DeserializeOwned + Send + 'static,
    {
        let subject = subject.name;
        let counter = Arc::clone(&self.counter);
        let handler = move |state: S, messages: Vec<jetstream::Message>| {
            handle_messages(subject, messages, handler, &state, &counter)
        };

        self.handlers.push((subject, Box::new(handler)));
    }

    pub fn add_bulk_handler<H, Fut, M>(&mut self, subject: &Subject<M>, handler: H)
    where
        H: Fn(S, Vec<Message<M>>) -> Fut + Copy + Send + Sync + 'static,
        Fut: Future<Output = Result<(), Exception>> + Send + 'static,
        M: DeserializeOwned + Send + 'static,
    {
        let subject = subject.name;
        let counter = Arc::clone(&self.counter);
        let handler = move |state: S, messages: Vec<jetstream::Message>| {
            handle_bulk_messages(subject, messages, handler, state, Arc::clone(&counter))
        };

        self.handlers.push((subject, Box::new(handler)));
    }

    pub async fn start(self, state: S, shutdown_signal: CancellationToken) {
        let Self { url, stream, durable_prefix, handlers, config, .. } = self;

        let connection = async_nats::connect(url).await.expect("failed to connect nats"); // fail fast on startup
        let context = jetstream::new(connection);
        let stream_handle =
            context.get_stream(stream).await.unwrap_or_else(|e| panic!("failed to get stream, error={e}")); // fail fast on startup

        let subjects: Vec<&str> = handlers.iter().map(|(subject, _)| *subject).collect();
        console!("nats consumer started, stream={stream}, subjects={subjects:?}");

        let mut tasks = JoinSet::new();
        for (subject, handler) in handlers {
            // durable names cannot contain '.', '*', '>' or whitespace.
            let durable = format!("{durable_prefix}-{}", subject.replace(['.', '*', '>'], "_"));
            let consumer = stream_handle
                .get_or_create_consumer(
                    durable.as_str(),
                    pull::Config {
                        durable_name: Some(durable.clone()),
                        filter_subject: subject.to_owned(),
                        ack_policy: AckPolicy::Explicit,
                        ack_wait: config.ack_wait,
                        max_deliver: config.max_deliver,
                        ..Default::default()
                    },
                )
                .await
                .expect("failed to create consumer"); // fail fast on startup

            tasks.spawn(consume_loop(subject, consumer, handler, state.clone(), shutdown_signal.clone(), config));
        }

        tasks.join_all().await;
        console!("nats consumer stopped, stream={stream}, subjects={subjects:?}");
    }
}

async fn consume_loop<S>(
    subject: &'static str,
    consumer: PullConsumer,
    handler: MessageHandler<S>,
    state: S,
    shutdown_signal: CancellationToken,
    config: ConsumerConfig,
) where
    S: Clone + Send + 'static,
{
    loop {
        match consumer.batch().max_messages(config.batch_max_messages).expires(config.batch_max_wait).messages().await {
            Ok(mut batch) => {
                let mut messages = Vec::new();
                while let Some(result) = batch.next().await {
                    match result {
                        Ok(message) => messages.push(message),
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
                if !messages.is_empty() {
                    handler(state.clone(), messages).await;
                }
            }
            Err(e) => {
                console!("ERROR {}", exception!(format!("failed to fetch messages, subject={subject}"), source = e));
                time::sleep(Duration::from_secs(5)).await;
            }
        }

        if shutdown_signal.is_cancelled() {
            return;
        }
    }
}

fn handle_messages<H, S, M, Fut>(
    subject: &'static str,
    messages: Vec<jetstream::Message>,
    handler: H,
    state: &S,
    counter: &Arc<Counter>,
) -> Pin<Box<dyn Future<Output = ()> + Send>>
where
    S: Clone + Send + 'static,
    H: Fn(S, Message<M>) -> Fut + Copy + Send + Sync + 'static,
    Fut: Future<Output = Result<(), Exception>> + Send + 'static,
    M: DeserializeOwned + Send + 'static,
{
    // no per-key chaining: NATS has no native key, and per-message nak redelivery breaks
    // in-process ordering anyway. one task per message; each acks/naks its own message.
    let mut handles = JoinSet::new();
    for message in messages {
        let state = state.clone();
        let counter = Arc::clone(counter);
        handles.spawn(async move {
            let _counter = counter.increase();
            handle_message(subject, message, handler, state).await;
        });
    }

    Box::pin(async move {
        handles.join_all().await;
    })
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
        if let Err(e) = raw.ack().await {
            return Err(exception!("failed to ack message", source = Exception::from_dyn(e.as_ref())));
        }
        result
    })
    .await;
}

fn handle_bulk_messages<H, S, M, Fut>(
    subject: &'static str,
    raw_messages: Vec<jetstream::Message>,
    handler: H,
    state: S,
    counter: Arc<Counter>,
) -> Pin<Box<dyn Future<Output = ()> + Send>>
where
    S: Send + 'static,
    H: Fn(S, Vec<Message<M>>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), Exception>> + Send + 'static,
    M: DeserializeOwned + Send + 'static,
{
    Box::pin(async move {
        let messages: Vec<Message<M>> = raw_messages.iter().map(Message::from_nats).collect();
        let ref_id: Option<Vec<String>> =
            messages.iter().map(|m| m.headers.get(REF_ID).map(String::to_owned)).collect();
        let _result = log::start_action("message", ref_id, async move {
            let _counter = counter.increase();
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
            if let Some(timestamp) = messages.iter().filter_map(|message| message.timestamp).min() {
                let lag = Utc::now() - timestamp;
                log!("lag={lag}");
            }
            let result = handler(state, messages).await;
            for raw in &raw_messages {
                if let Err(e) = raw.ack().await {
                    error!(
                        error_code = "NATS_ACK_FAILED",
                        "{}",
                        exception!(
                            format!("failed to ack message, subject={subject}"),
                            source = Exception::from_dyn(e.as_ref())
                        )
                    );
                }
            }
            result
        })
        .await;
    })
}
