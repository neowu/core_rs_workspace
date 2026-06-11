use std::future::Future;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::Duration;

use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time;

use crate::exception::Exception;
use crate::log;
use crate::log::current_action_id;
use crate::log::metrics::Counter;
use crate::log::metrics::Metrics;

#[macro_export]
macro_rules! spawn_action {
    ($name:expr, $task:expr) => {
        $crate::task::__spawn_action($name, concat!(file!(), ":", line!()), $task)
    };
}

static EXECUTOR: LazyLock<TaskExecutor> =
    LazyLock::new(|| TaskExecutor { running_tasks: Mutex::new(Vec::new()), empty: Notify::new() });

struct TaskExecutor {
    running_tasks: Mutex<Vec<String>>,
    empty: Notify,
}

impl TaskExecutor {
    async fn wait(&self) {
        loop {
            // register the waiter BEFORE checking, to avoid missing a wakeup
            let notified = self.empty.notified();
            if self.running_tasks.lock().unwrap().is_empty() {
                break;
            }
            notified.await;
        }
    }
}

struct TaskGuard {
    task_name: String,
}

impl Drop for TaskGuard {
    fn drop(&mut self) {
        let mut tasks = EXECUTOR.running_tasks.lock().unwrap();
        if let Some(pos) = tasks.iter().position(|name| name == &self.task_name) {
            tasks.swap_remove(pos);
        }
        if tasks.is_empty() {
            EXECUTOR.empty.notify_waiters();
        }
    }
}

#[doc(hidden)]
#[inline]
pub fn __spawn_action<T, R>(name: &'static str, location: &'static str, task: T) -> JoinHandle<Result<R, Exception>>
where
    T: Future<Output = Result<R, Exception>> + Send + 'static,
    R: Send + Sync + 'static,
{
    let task_name = format!("task:{name}@{location}");
    EXECUTOR.running_tasks.lock().unwrap().push(task_name.clone());

    let ref_id = current_action_id().map(|id| vec![id]);
    tokio::spawn(async move {
        // hold the guard for the whole task; dropped on completion or on abort/cancel
        let _guard = TaskGuard { task_name };
        let _counter = TASK_COUNTER.get().map(Counter::increase);

        log::start_action("task", ref_id, async {
            context!(task = name, location = location);
            task.await
        })
        .await
    })
}

#[doc(hidden)]
#[inline]
pub fn __spawn<T>(task_name: String, task: T)
where
    T: Future<Output = ()> + Send + 'static,
{
    EXECUTOR.running_tasks.lock().unwrap().push(task_name.clone());
    let guard = TaskGuard { task_name };

    tokio::spawn(async {
        let _guard = guard;
        task.await;
    });
}

static TASK_COUNTER: OnceLock<Counter> = OnceLock::new();

pub fn task_collector() -> impl Fn(&mut Metrics) {
    TASK_COUNTER.set(Counter::new()).unwrap_or_else(|_| panic!("task_collector can only be called once"));
    task_metrics
}

fn task_metrics(metrics: &mut Metrics) {
    if let Some(counter) = TASK_COUNTER.get() {
        let max = counter.max();
        metrics.stats.push(("active_tasks", max as u64));
    }
}

pub async fn shutdown(timeout: Duration) {
    if time::timeout(timeout, EXECUTOR.wait()).await.is_ok() {
        console!("tasks finished");
    } else {
        let tasks = EXECUTOR.running_tasks.lock().unwrap();
        console!("WARN some of tasks failed to finish, running_tasks={:?}", tasks.iter());
    }
}
