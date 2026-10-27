use std::marker::PhantomData;

use framework::console;
pub use iggy;
use iggy::prelude::Client as _;
use iggy::prelude::Identifier;
use iggy::prelude::IggyClient;
use iggy::prelude::IggyError;
use iggy::prelude::StreamClient as _;
use iggy::prelude::TopicClient as _;
use iggy::prelude::TopicCreateOptions;

pub mod consumer;
pub mod producer;

pub struct Topic<T> {
    pub name: &'static str,
    _marker: PhantomData<T>,
}

impl<T> Topic<T> {
    pub const fn new(name: &'static str) -> Topic<T> {
        Topic { name, _marker: PhantomData }
    }
}

type Header = &'static str;
const REF_ID: Header = "ref_id";
const CLIENT: Header = "client";
// iggy only hashes the key to pick a partition and does not store it, so the framework carries it as a header
const KEY: Header = "key";

// partitions of a topic the framework creates; existing topics are left as they are
const PARTITIONS: u32 = 4;

// url is a connection string with credentials, e.g. "iggy://user:password@localhost:8090", which signs in on connect.
// a connection serves one request at a time, so the producer and each consumer topic get their own
async fn connect(url: &str) -> IggyClient {
    console!("connect to iggy, url={}", url.rsplit('@').next().unwrap_or_default());
    let client = IggyClient::from_connection_string(url).expect("invalid iggy connection string"); // fail fast on startup
    client.connect().await.expect("failed to connect iggy");
    client
}

// iggy does not auto create streams or topics; "already exists" is success, so concurrent creators are safe
async fn create_stream_if_not_exists(client: &IggyClient, stream: &str) -> Result<(), IggyError> {
    match client.create_stream(stream).await {
        Ok(_) | Err(IggyError::StreamNameAlreadyExists(_)) => Ok(()),
        Err(e) => Err(e),
    }
}

async fn create_topic_if_not_exists(client: &IggyClient, stream: &Identifier, topic: &str) -> Result<(), IggyError> {
    let options = TopicCreateOptions { partitions_count: Some(PARTITIONS), ..TopicCreateOptions::default() };
    match client.create_topic(stream, topic, &options).await {
        Ok(_) | Err(IggyError::TopicNameAlreadyExists(..)) => Ok(()),
        Err(e) => Err(e),
    }
}
