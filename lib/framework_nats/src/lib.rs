use std::marker::PhantomData;

pub mod consumer;
pub mod producer;

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
