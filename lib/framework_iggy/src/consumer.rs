use std::any::type_name;
use std::borrow::Cow;
use std::collections::HashMap;
use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use framework::console;
use framework::context;
use framework::exception;
use framework::exception::Exception;
use framework::json::from_json;
use framework::log;
use framework::metrics::Counter;
use framework::metrics::Metrics;
use framework::stats;
use framework::time::DateTime;
use futures::FutureExt as _;
use futures::StreamExt as _;
use iggy::prelude::AutoCommit;
use iggy::prelude::Identifier;
use iggy::prelude::IggyConsumer;
use iggy::prelude::IggyDuration;
use iggy::prelude::IggyError;
use iggy::prelude::IggyMessage;
use iggy::prelude::PollingStrategy;
use serde::de::DeserializeOwned;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio::time;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::CLIENT;
use crate::KEY;
use crate::REF_ID;
use crate::Topic;
use crate::connect;
use crate::create_stream_if_not_exists;
use crate::create_topic_if_not_exists;

// decoded message handed to call-site handlers
pub struct Message<T> {
    pub key: Option<String>,
    pub payload: T,
}

// iggy message with the framework headers parsed once
struct RawMessage {
    key: Option<String>,
    ref_id: Option<String>,
    client: Option<String>,
    payload: Bytes,
    // micros, set by the producing client
    origin_timestamp: u64,
}

type MessageHandler<S> = Box<dyn Fn(S, Vec<RawMessage>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

pub struct ConsumerConfig {
    // max in-flight handlers (semaphore size) across topics: each add_handler key group and each
    // add_bulk_handler call holds one permit.
    pub max_concurrency: usize,
    pub poll_max_wait_time: Duration,
    // also the iggy poll batch_length, see spec/iggy.md
    pub poll_max_records: u32,
    // min gap between two poll requests of one topic; iggy has no long poll, so it also paces polling an idle topic
    pub poll_interval: Duration,
}

impl Default for ConsumerConfig {
    fn default() -> ConsumerConfig {
        ConsumerConfig {
            max_concurrency: 100,
            poll_max_wait_time: Duration::from_secs(1),
            poll_max_records: 1000,
            poll_interval: Duration::from_millis(10),
        }
    }
}

pub struct MessageConsumer<S> {
    url: String,
    stream: &'static str,
    group: &'static str,
    handlers: HashMap<&'static str, MessageHandler<S>>,
    poll_max_wait_time: Duration,
    poll_max_records: u32,
    poll_interval: Duration,
    counter: Arc<Counter>,
    semaphore: Arc<Semaphore>,
}

impl<S> MessageConsumer<S>
where
    S: Clone + Send + Sync + 'static,
{
    // group usually be env!("CARGO_BIN_NAME")
    pub fn new(url: String, stream: &'static str, group: &'static str, config: &ConsumerConfig) -> MessageConsumer<S> {
        MessageConsumer {
            url,
            stream,
            group,
            handlers: HashMap::new(),
            poll_max_wait_time: config.poll_max_wait_time,
            poll_max_records: config.poll_max_records,
            poll_interval: config.poll_interval,
            counter: Arc::default(),
            semaphore: Arc::new(Semaphore::new(config.max_concurrency)),
        }
    }

    pub fn metrics(&self) -> impl Fn(&mut Metrics) + use<S> {
        let counter = Arc::clone(&self.counter);
        move |metrics| {
            metrics.add_stat("active_message_handlers", counter.max() as u64);
        }
    }

    pub fn add_handler<H, Fut, M>(&mut self, topic: &Topic<M>, handler: H)
    where
        H: Fn(S, Message<M>) -> Fut + Copy + Send + Sync + 'static,
        Fut: Future<Output = Result<(), Exception>> + Send + 'static,
        M: DeserializeOwned + Send + 'static,
    {
        let name = topic.name;
        let counter = Arc::clone(&self.counter);
        let semaphore = Arc::clone(&self.semaphore);
        let wrapper: MessageHandler<S> = Box::new(move |state: S, messages: Vec<RawMessage>| {
            Box::pin(handle_messages(name, messages, handler, state, Arc::clone(&counter), Arc::clone(&semaphore)))
        });
        self.handlers.insert(name, wrapper);
    }

    pub fn add_bulk_handler<H, Fut, M>(&mut self, topic: &Topic<M>, handler: H)
    where
        H: Fn(S, Vec<Message<M>>) -> Fut + Copy + Send + Sync + 'static,
        Fut: Future<Output = Result<(), Exception>> + Send + 'static,
        M: DeserializeOwned + Send + 'static,
    {
        let name = topic.name;
        let counter = Arc::clone(&self.counter);
        let semaphore = Arc::clone(&self.semaphore);
        let wrapper: MessageHandler<S> = Box::new(move |state: S, messages: Vec<RawMessage>| {
            Box::pin(handle_bulk_messages(name, messages, handler, state, Arc::clone(&counter), Arc::clone(&semaphore)))
        });
        self.handlers.insert(name, wrapper);
    }

    // an iggy consumer reads one topic, so each topic runs its own poll loop
    pub async fn start(self, state: S, shutdown_signal: CancellationToken) {
        let topics: Vec<&str> = self.handlers.keys().copied().collect();
        console!("start iggy consumer, stream={}, group={}, topics={:?}", self.stream, self.group, topics);

        let mut loops = JoinSet::new();
        for (topic, handler) in self.handlers {
            // own connection per topic: a connection serves one request at a time, so a shared one would hold
            // every topic's commits behind another topic's poll
            let client = connect(&self.url).await;
            create_stream_if_not_exists(&client, self.stream).await.expect("failed to create stream"); // fail fast on startup
            create_topic_if_not_exists(&client, &Identifier::named(self.stream).expect("invalid stream name"), topic)
                .await
                .expect("failed to create topic");
            let mut consumer = client
                .consumer_group(self.group, self.stream, topic)
                .expect("invalid stream or topic name") // fail fast on startup
                .auto_commit(AutoCommit::Disabled)
                .polling_strategy(PollingStrategy::next())
                .batch_length(self.poll_max_records)
                .poll_interval(IggyDuration::from(self.poll_interval))
                .build();
            consumer.init().await.expect("failed to init iggy consumer"); // fail fast on startup
            let consume = consume(
                consumer,
                topic,
                handler,
                state.clone(),
                self.poll_max_wait_time,
                self.poll_max_records,
                shutdown_signal.clone(),
            );
            // the client outlives the consumer that shares its connection
            loops.spawn(async move {
                consume.await;
                drop(client);
            });
        }
        while let Some(result) = loops.join_next().await {
            if let Err(e) = result {
                console!("ERROR iggy consumer panicked, error={e:?}");
            }
        }
        console!("iggy consumer stopped, topics={:?}", topics);
    }
}

async fn consume<S>(
    mut consumer: IggyConsumer,
    topic: &'static str,
    handler: MessageHandler<S>,
    state: S,
    poll_max_wait_time: Duration,
    poll_max_records: u32,
    shutdown_signal: CancellationToken,
) where
    S: Clone + Send + 'static,
{
    loop {
        let mut messages = Vec::new();
        // highest offset polled per partition, committed once the batch is handled
        let mut offsets: HashMap<u32, u64> = HashMap::new();
        let result = poll_messages(
            &mut consumer,
            &mut messages,
            &mut offsets,
            poll_max_wait_time,
            poll_max_records,
            &shutdown_signal,
        )
        .await;

        // messages polled before an error are still handled, otherwise the next commit would skip them
        if !messages.is_empty() {
            // spawned, so a panicking handler is caught here and the batch is still committed
            if let Err(e) = tokio::spawn(handler(state.clone(), messages)).await {
                console!("ERROR message handler panicked, topic={topic}, error={e:?}");
            }
            for (partition_id, offset) in offsets {
                if let Err(e) = consumer.store_offset(offset, Some(partition_id)).await {
                    console!("ERROR failed to commit messages, topic={topic}, partition={partition_id}, error={e:?}");
                }
            }
        }

        // the consumer already waited polling_retry_interval before yielding the error, no extra backoff
        if let Err(e) = result {
            console!("ERROR failed to poll messages, topic={topic}, error={e:?}");
        }

        if shutdown_signal.is_cancelled() {
            // leaves the group, so its partitions are reassigned at once; AutoCommit::Disabled commits nothing here
            if let Err(e) = consumer.shutdown().await {
                console!("ERROR failed to shutdown iggy consumer, topic={topic}, error={e:?}");
            }
            return;
        }
    }
}

// collects until max_records, max_wait_time, or shutdown; IggyConsumer keeps its in-flight poll across
// dropped next() calls, so racing it is cancel safe
async fn poll_messages(
    consumer: &mut IggyConsumer,
    messages: &mut Vec<RawMessage>,
    offsets: &mut HashMap<u32, u64>,
    max_wait_time: Duration,
    max_records: u32,
    shutdown_signal: &CancellationToken,
) -> Result<(), IggyError> {
    let deadline = Instant::now() + max_wait_time;
    let mut count = 0;
    while count < max_records {
        tokio::select! {
            result = consumer.next() => {
                let Some(received) = result else { break }; // only after shutdown()
                let received = received?;
                offsets.insert(received.partition_id, received.message.header.offset);
                messages.push(raw_message(received.message));
                count += 1;
            }
            () = time::sleep_until(deadline) => break,
            () = shutdown_signal.cancelled() => break,
        }
    }
    Ok(())
}

fn raw_message(message: IggyMessage) -> RawMessage {
    let (mut key, mut ref_id, mut client) = (None, None, None);
    // headers are framework-written utf8 strings; a malformed header block is treated as absent
    if let Ok(Some(headers)) = message.user_headers_map() {
        for (name, value) in &headers {
            let field = match name.as_str() {
                Ok(KEY) => &mut key,
                Ok(REF_ID) => &mut ref_id,
                Ok(CLIENT) => &mut client,
                _ => continue,
            };
            *field = value.as_str().ok().map(str::to_owned);
        }
    }
    RawMessage { key, ref_id, client, payload: message.payload, origin_timestamp: message.header.origin_timestamp }
}

// the bulk handler holds one permit, as a key group does
async fn handle_bulk_messages<H, S, M, Fut>(
    topic: &'static str,
    raw_messages: Vec<RawMessage>,
    handler: H,
    state: S,
    counter: Arc<Counter>,
    semaphore: Arc<Semaphore>,
) where
    S: Send + 'static,
    H: Fn(S, Vec<Message<M>>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), Exception>> + Send + 'static,
    M: DeserializeOwned + Send + 'static,
{
    // acquired outside the action, so the wait is not counted in its elapsed
    let _permit = semaphore.acquire().await.expect("semaphore should not close");
    let ref_id = distinct_values(&raw_messages, |message| message.ref_id.as_deref());

    let _result = log::action("message", ref_id, async move {
        let _counter = counter.increase();
        context!(topic = topic, fn = type_name::<H>());
        let mut bytes = 0;
        let mut messages: Vec<Message<M>> = Vec::with_capacity(raw_messages.len());
        for raw in &raw_messages {
            let payload = payload(raw);
            log!("[message] key={:?}, payload={}", raw.key, payload);
            bytes += payload.len();
            match from_json::<M>(&payload) {
                Ok(payload) => messages.push(Message { key: raw.key.clone(), payload }),
                Err(e) => {
                    log!(exception = exception!("failed to decode message", code = "IGGY_INVALID_MESSAGE", source = e));
                }
            }
        }
        stats!(iggy_read_messages = messages.len(), iggy_read_bytes = bytes);
        if let Some(timestamp) = raw_messages.iter().filter_map(timestamp).min() {
            log!("[message] timestamp={}", timestamp.to_rfc3339());
            let lag = (DateTime::now() - timestamp).as_nanos();
            if lag > 0 {
                stats!(iggy_consumer_lag = lag);
            }
        }
        if let Some(clients) = distinct_values(&raw_messages, |message| message.client.as_deref()) {
            context!(client = clients);
        }
        handler(state, messages).await
    })
    .await;
}

// messages with the same key are handled sequentially in order, each key group (or unkeyed message) runs in its
// own task, bounded by the consumer semaphore
async fn handle_messages<H, S, M, Fut>(
    topic: &'static str,
    messages: Vec<RawMessage>,
    handler: H,
    state: S,
    counter: Arc<Counter>,
    semaphore: Arc<Semaphore>,
) where
    S: Clone + Send + 'static,
    H: Fn(S, Message<M>) -> Fut + Copy + Send + Sync + 'static,
    Fut: Future<Output = Result<(), Exception>> + Send + 'static,
    M: DeserializeOwned + Send + 'static,
{
    let mut key_groups: HashMap<String, Vec<RawMessage>> = HashMap::new();
    let mut groups: Vec<Vec<RawMessage>> = Vec::new();
    for message in messages {
        if let Some(ref key) = message.key {
            key_groups.entry(key.clone()).or_default().push(message);
        } else {
            groups.push(vec![message]);
        }
    }
    groups.extend(key_groups.into_values());

    let mut handles = JoinSet::new();
    for group in groups {
        let permit = Arc::clone(&semaphore).acquire_owned().await.expect("semaphore should not close");
        let state = state.clone();
        let counter = Arc::clone(&counter);
        handles.spawn(async move {
            let _permit = permit;
            let _counter = counter.increase();
            for message in group {
                handle_message(topic, message, handler, state.clone()).await;
            }
        });
    }
    // not join_all, which re-panics on the first panicked group and aborts the others mid-flight
    while let Some(result) = handles.join_next().await {
        if let Err(e) = result {
            console!("ERROR message handler panicked, topic={topic}, error={e:?}");
        }
    }
}

fn handle_message<H, S, M, Fut>(
    topic: &'static str,
    raw_message: RawMessage,
    handler: H,
    state: S,
) -> impl Future<Output = ()>
where
    H: Fn(S, Message<M>) -> Fut,
    Fut: Future<Output = Result<(), Exception>>,
    M: DeserializeOwned,
{
    let ref_id = raw_message.ref_id.clone().map(|id| vec![id]);
    log::action("message", ref_id, async move {
        let payload = payload(&raw_message);
        context!(topic = topic, key = format!("{:?}", raw_message.key), fn = type_name::<H>());
        log!("[message] payload={}", payload);
        stats!(iggy_read_messages = 1, iggy_read_bytes = payload.len());
        if let Some(timestamp) = timestamp(&raw_message) {
            log!("[message] timestamp={}", timestamp.to_rfc3339());
            let lag = (DateTime::now() - timestamp).as_nanos();
            if lag > 0 {
                stats!(iggy_consumer_lag = lag);
            }
        }
        if let Some(ref client) = raw_message.client {
            context!(client = client);
        }
        let payload: M = from_json(&payload)
            .map_err(|e| exception!("failed to decode message", code = "IGGY_INVALID_MESSAGE", source = e))?;
        handler(state, Message { key: raw_message.key.clone(), payload }).await
    })
    .map(drop)
}

// distinct values across the batch, messages without the header are skipped
fn distinct_values(messages: &[RawMessage], value: impl Fn(&RawMessage) -> Option<&str>) -> Option<Vec<String>> {
    let values: HashSet<&str> = messages.iter().filter_map(value).collect();
    (!values.is_empty()).then(|| values.into_iter().map(str::to_owned).collect())
}

// lossy utf8 borrows valid text, so the logged and parsed payload is never copied
fn payload(message: &RawMessage) -> Cow<'_, str> {
    String::from_utf8_lossy(&message.payload)
}

fn timestamp(message: &RawMessage) -> Option<DateTime> {
    (message.origin_timestamp > 0)
        .then(|| DateTime::from_unix_timestamp_nanos(i128::from(message.origin_timestamp) * 1000).ok())
        .flatten()
}
