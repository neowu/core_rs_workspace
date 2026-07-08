use std::marker::PhantomData;

pub use async_nats;

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
pub async fn connect(url: String) -> async_nats::Client {
    async_nats::connect(url).await.expect("failed to connect nats") // fail fast on startup
}
