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

use crate::exception::Exception;

pub(crate) trait ResourceManager {
    type Target: Sized;

    async fn create(&self) -> Result<Self::Target, Exception>;
    async fn is_valid(item: &Self::Target) -> bool;
    fn is_closed(item: &Self::Target) -> bool;
}

struct Resource<T> {
    item: T,
    return_time: Instant,
}

pub(crate) struct ResourcePool<R>
where
    R: ResourceManager,
{
    inner: Arc<PoolInner<R>>,
}

struct PoolInner<R>
where
    R: ResourceManager,
{
    storage: Mutex<VecDeque<Resource<R::Target>>>,
    semaphore: Arc<Semaphore>,
    manager: R,
    max_alive_time: Duration,
}

impl<R> Clone for ResourcePool<R>
where
    R: ResourceManager,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<R> ResourcePool<R>
where
    R: ResourceManager,
{
    pub fn new(manager: R, capacity: usize, max_alive_time: Duration) -> Self {
        Self {
            inner: Arc::new(PoolInner {
                storage: Mutex::new(VecDeque::with_capacity(capacity)),
                semaphore: Arc::new(Semaphore::new(capacity)),
                manager,
                max_alive_time,
            }),
        }
    }

    pub async fn get_with_timeout(&self, timeout: Duration) -> Result<ResourceGuard<R>, Exception> {
        let permit = match time::timeout(timeout, self.inner.semaphore.clone().acquire_owned()).await {
            Ok(Ok(permit)) => permit,
            Ok(Err(_)) => return Err(exception!(message = "pool is closed")),
            Err(_) => return Err(exception!(message = "timeout")),
        };

        let inner = loop {
            let candidate = {
                let mut storage = self.inner.storage.lock().unwrap();
                storage.pop_front()
            };

            match candidate {
                None => break self.inner.manager.create().await?,
                Some(res) => {
                    let exceeded_max_alive_time = res.return_time.elapsed() > self.inner.max_alive_time;

                    if !exceeded_max_alive_time || R::is_valid(&res.item).await {
                        break res.item;
                    }
                }
            }
        };

        Ok(ResourceGuard {
            resource: Some(Resource {
                item: inner,
                return_time: Instant::now(),
            }),
            pool: Arc::clone(&self.inner),
            _permit: permit,
        })
    }
}

pub(crate) struct ResourceGuard<R>
where
    R: ResourceManager,
{
    resource: Option<Resource<R::Target>>,
    pool: Arc<PoolInner<R>>,
    _permit: OwnedSemaphorePermit,
}

impl<R> Deref for ResourceGuard<R>
where
    R: ResourceManager,
{
    type Target = R::Target;

    fn deref(&self) -> &Self::Target {
        &self
            .resource
            .as_ref()
            .expect("ResourceGuard always holds a resource")
            .item
    }
}

impl<R> DerefMut for ResourceGuard<R>
where
    R: ResourceManager,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self
            .resource
            .as_mut()
            .expect("ResourceGuard always holds a resource")
            .item
    }
}

impl<R> Drop for ResourceGuard<R>
where
    R: ResourceManager,
{
    fn drop(&mut self) {
        if let Some(mut resource) = self.resource.take()
            && !R::is_closed(&resource.item)
        {
            resource.return_time = Instant::now();
            let mut storage = self.pool.storage.lock().unwrap();
            storage.push_back(resource);
        }
    }
}
