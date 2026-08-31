use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::time;
use tokio_util::task::TaskTracker;

use crate::exception::Exception;
use crate::log;
use crate::metrics::Counter;
use crate::metrics::Metrics;

static EXECUTOR: OnceLock<Executor> = OnceLock::new();

pub fn start_executor() -> &'static Executor {
    EXECUTOR.get_or_init(Executor::default)
}

#[doc(hidden)]
pub fn __spawn_action<T, R>(name: &'static str, location: &'static str, task: T)
where
    T: Future<Output = Result<R, Exception>> + Send + 'static,
    R: Send + Sync + 'static,
{
    if let Some(executor) = EXECUTOR.get() {
        executor.spawn(name, location, task);
    } else {
        panic!("executor not initialized");
    }
}

#[macro_export]
macro_rules! spawn_action {
    ($name:expr, $task:expr) => {
        $crate::task::__spawn_action($name, concat!(file!(), ":", line!()), $task)
    };
}

#[derive(Default)]
pub struct Executor {
    executor: TaskExecutor,
    counter: Arc<Counter>,
}

impl Executor {
    fn spawn<T, R>(&self, name: &'static str, location: &'static str, task: T)
    where
        T: Future<Output = Result<R, Exception>> + Send + 'static,
        R: Send + Sync + 'static,
    {
        let task_name = format!("task:{name}@{location}");
        let ref_ids = log::current_action_id().map(|id| vec![id]);

        let counter = Arc::clone(&self.counter);
        self.executor.spawn(task_name, async move {
            let _counter = counter.increase();
            let _result = log::action("task", ref_ids, async {
                context!(task = name, location = location);
                task.await
            })
            .await;
        });
    }

    pub fn metrics(&self) -> impl Fn(&mut Metrics) + Send + 'static {
        let counter = Arc::clone(&self.counter);
        move |metrics| {
            metrics.stats.push(("active_tasks", counter.max() as u64));
        }
    }

    pub async fn shutdown(&self, timeout: Duration) {
        if let Some(aborted) = self.executor.shutdown(timeout).await {
            console!("WARN executor tasks aborted, tasks={aborted:?}");
        } else {
            console!("executor stopped");
        }
    }
}

#[derive(Default)]
pub struct TaskExecutor {
    tracker: TaskTracker,
    tasks: Arc<Mutex<HashMap<u64, String>>>,
    next_id: Arc<AtomicU64>,
}

struct TaskGuard {
    tasks: Arc<Mutex<HashMap<u64, String>>>,
    id: u64,
}

impl Drop for TaskGuard {
    fn drop(&mut self) {
        self.tasks.lock().unwrap().remove(&self.id);
    }
}

impl TaskExecutor {
    pub fn spawn<F>(&self, name: String, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.tasks.lock().unwrap().insert(id, name);
        let tasks = Arc::clone(&self.tasks);
        self.tracker.spawn(async move {
            let _guard = TaskGuard { tasks, id };
            task.await;
        });
    }

    pub async fn shutdown(&self, timeout: Duration) -> Option<Vec<String>> {
        self.tracker.close();
        if time::timeout(timeout, self.tracker.wait()).await.is_ok() {
            return None;
        }
        let aborted: Vec<String> = self.tasks.lock().unwrap().values().cloned().collect();
        (!aborted.is_empty()).then_some(aborted)
    }
}
