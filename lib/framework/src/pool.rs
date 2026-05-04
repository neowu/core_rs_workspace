use std::collections::VecDeque;
use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tokio::time;
use tracing::warn;

use crate::exception::Exception;

pub(crate) trait ResourceManager {
    type Target: Sized;

    async fn create(&self) -> Result<Self::Target, Exception>;
    async fn is_valid(item: &Self::Target) -> bool;
    fn is_closed(item: &Self::Target) -> bool;
}

struct Resource<T> {
    item: T,
    created_time: Instant,
    return_time: Instant,
}

pub(crate) struct ResourcePool<R>
where
    R: ResourceManager,
{
    storage: Mutex<VecDeque<Resource<R::Target>>>,
    semaphore: Arc<Semaphore>,
    manager: R,
    max_valid_window: Duration,
    max_life_time: Duration,
    checkout_timeout: Duration,
}

impl<R> ResourcePool<R>
where
    R: ResourceManager,
{
    pub(crate) fn new(
        manager: R,
        capacity: usize,
        max_valid_window: Duration,
        max_life_time: Duration,
        checkout_timeout: Duration,
    ) -> Self {
        Self {
            storage: Mutex::new(VecDeque::with_capacity(capacity)),
            semaphore: Arc::new(Semaphore::new(capacity)),
            manager,
            max_valid_window,
            max_life_time,
            checkout_timeout,
        }
    }

    pub(crate) async fn get_with_timeout(&'_ self) -> Result<ResourceGuard<'_, R>, Exception> {
        let permit = match time::timeout(self.checkout_timeout, Arc::clone(&self.semaphore).acquire_owned()).await {
            Ok(Ok(permit)) => permit,
            Ok(Err(_)) => return Err(exception!(message = "pool is closed")),
            Err(_) => return Err(exception!(message = "timeout")),
        };

        let item = loop {
            let candidate = {
                let mut storage = self.storage.lock().expect("only if lock if poisoned");
                storage.pop_front()
            };

            match candidate {
                None => break self.manager.create().await?,
                Some(res) => {
                    if res.return_time.elapsed() < self.max_valid_window {
                        break res.item;
                    }

                    let is_valid = R::is_valid(&res.item).await;
                    if is_valid {
                        break res.item;
                    }
                    warn!("resource is not valid, try next");
                }
            }
        };

        let now = Instant::now();
        Ok(ResourceGuard {
            resource: Some(Resource { item, created_time: now, return_time: now }),
            pool: self,
            _permit: permit,
        })
    }
}

pub(crate) struct ResourceGuard<'a, R>
where
    R: ResourceManager,
{
    resource: Option<Resource<R::Target>>,
    pool: &'a ResourcePool<R>,
    _permit: OwnedSemaphorePermit,
}

impl<R> Deref for ResourceGuard<'_, R>
where
    R: ResourceManager,
{
    type Target = R::Target;

    fn deref(&self) -> &Self::Target {
        &self.resource.as_ref().expect("ResourceGuard always holds a resource").item
    }
}

impl<R> DerefMut for ResourceGuard<'_, R>
where
    R: ResourceManager,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.resource.as_mut().expect("ResourceGuard always holds a resource").item
    }
}

impl<R> Drop for ResourceGuard<'_, R>
where
    R: ResourceManager,
{
    fn drop(&mut self) {
        if let Some(mut resource) = self.resource.take()
            && !R::is_closed(&resource.item)
            && resource.created_time.elapsed() < self.pool.max_life_time
        {
            resource.return_time = Instant::now();
            let mut storage = self.pool.storage.lock().expect("only if lock if poisoned");
            storage.push_back(resource);
        }
    }
}
