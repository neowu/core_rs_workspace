use std::collections::HashMap;
use std::future::Future;
use std::mem;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::Duration;

use tokio::task::Id;
use tokio::task::JoinSet;
use tokio::time;

use crate::exception::Exception;
use crate::log;
use crate::log::current_action_id;
use crate::log::metrics::Counter;
use crate::log::metrics::Metrics;

// the global executor for fire-and-forget tasks; callers that want their own lifecycle hold a TaskExecutor directly
static EXECUTOR: LazyLock<Mutex<TaskExecutor>> = LazyLock::new(Mutex::default);

#[derive(Default)]
pub struct TaskExecutor {
    set: JoinSet<()>,
    names: HashMap<Id, String>,
}

impl TaskExecutor {
    pub fn spawn<F>(&mut self, name: String, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        // reap tasks that already finished, so `names` (and the set) don't grow unbounded
        while let Some(result) = self.set.try_join_next_with_id() {
            let id = match result {
                Ok((id, ())) => id,
                Err(error) => error.id(), // panicked or cancelled
            };
            self.names.remove(&id);
        }

        let handle = self.set.spawn(task);
        self.names.insert(handle.id(), name);
    }

    /// Wait for all spawned tasks to finish, up to `timeout`. Any tasks still
    /// running past the deadline are aborted; their names are returned.
    pub async fn shutdown(mut self, timeout: Duration) -> Option<Vec<String>> {
        let _result = time::timeout(timeout, async {
            while let Some(result) = self.set.join_next_with_id().await {
                let id = match result {
                    Ok((id, ())) => id,
                    Err(error) => error.id(), // panicked or cancelled
                };
                self.names.remove(&id);
            }
        })
        .await;

        self.set.abort_all();
        let aborted: Vec<String> = self.names.into_values().collect();
        (!aborted.is_empty()).then_some(aborted)
    }
}

#[macro_export]
macro_rules! spawn_action {
    ($name:expr, $task:expr) => {
        $crate::task::__spawn_action($name, concat!(file!(), ":", line!()), $task)
    };
}

#[doc(hidden)]
#[inline]
pub fn __spawn_action<T, R>(name: &'static str, location: &'static str, task: T)
where
    T: Future<Output = Result<R, Exception>> + Send + 'static,
    R: Send + Sync + 'static,
{
    let task_name = format!("task:{name}@{location}");
    let ref_id = current_action_id().map(|id| vec![id]);

    EXECUTOR.lock().unwrap().spawn(task_name, async move {
        let _counter = TASK_COUNTER.get().map(Counter::increase);

        // start_action logs the Exception on failure, so the Result can be discarded here
        let _result = log::start_action("task", ref_id, async {
            context!(task = name, location = location);
            task.await
        })
        .await;
    });
}

// counts only the global EXECUTOR's tasks; per-caller TaskExecutors track their own load separately
static TASK_COUNTER: OnceLock<Counter> = OnceLock::new();

pub fn task_metrics() -> impl Fn(&mut Metrics) {
    TASK_COUNTER.set(Counter::new()).unwrap_or_else(|_| panic!("task_metrics can only be called once"));
    |metrics| {
        if let Some(counter) = TASK_COUNTER.get() {
            metrics.stats.push(("active_tasks", counter.max() as u64));
        }
    }
}

pub async fn shutdown(timeout: Duration) {
    // swap the global executor out under the lock, then drain it without holding the lock across await
    let executor = mem::take(&mut *EXECUTOR.lock().unwrap());
    if let Some(aborted) = executor.shutdown(timeout).await {
        console!("WARN tasks aborted, tasks={aborted:?}");
    } else {
        console!("tasks finished");
    }
}
