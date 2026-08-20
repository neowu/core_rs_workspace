use std::marker::PhantomData;

pub use async_nats;
use async_nats::Client;
use async_nats::HeaderMap;
use framework::console;
use framework::log::with_current_action;

pub mod consumer;
pub mod producer;
pub mod service;

pub struct Subject<T> {
    pub name: &'static str,
    _marker: PhantomData<T>,
}

impl<T> Subject<T> {
    pub const fn new(name: &'static str) -> Self {
        Self { name, _marker: PhantomData }
    }
}

type Header = &'static str;
const REF_ID: Header = "ref_id";
const CLIENT: Header = "client";
const ERROR: Header = "error";

// one connection can be shared by services, service clients and producers within a process
pub async fn connect(url: &str) -> Client {
    console!("connect to nats, url={url}");
    async_nats::connect(url).await.expect("failed to connect nats") // fail fast on startup
}

fn link_context() -> HeaderMap {
    let mut headers = HeaderMap::new();
    if let Some((ref_id, app)) = with_current_action(|action| (action.id.clone(), action.app)) {
        headers.insert(REF_ID, ref_id);
        headers.insert(CLIENT, app);
    }
    headers
}
