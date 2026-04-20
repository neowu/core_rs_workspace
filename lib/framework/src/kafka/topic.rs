use std::marker::PhantomData;

pub struct Topic<T> {
    pub name: &'static str,
    _marker: PhantomData<T>,
}

impl<T> Topic<T> {
    pub fn new(name: &'static str) -> Self {
        Self { name, _marker: PhantomData }
    }
}
