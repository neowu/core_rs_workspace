use std::future::Future;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::time::Duration;

use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time;
use tracing::info;
use tracing::warn;

use crate::exception::Exception;
use crate::log;
use crate::log::current_action_id;

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
    let guard = TaskGuard { task_name };

    let ref_id = current_action_id();
    tokio::spawn(async move {
        // hold the guard for the whole task; dropped on completion or on abort/cancel
        let _guard = guard;
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

pub async fn shutdown(timeout: Duration) {
    if time::timeout(timeout, EXECUTOR.wait()).await.is_ok() {
        info!("tasks finished");
    } else {
        let tasks = EXECUTOR.running_tasks.lock().unwrap();
        warn!(running_tasks = ?tasks.iter(), "some of tasks failed to finish");
    }
}
