use std::future::Future;
use std::sync::LazyLock;

use tokio::task::JoinHandle;
use tokio_util::task::TaskTracker;
use tracing::info;

use crate::exception::Exception;
use crate::log;
use crate::log::current_action_id;

static TASK_TRACKER: LazyLock<TaskTracker> = LazyLock::new(TaskTracker::new);

#[macro_export]
macro_rules! spawn_action {
    ($name:expr, $task:expr) => {
        $crate::task::__spawn_action($name, concat!(file!(), ":", line!()), $task)
    };
}

#[doc(hidden)]
#[inline]
pub fn __spawn_action<T, R>(name: &'static str, location: &'static str, task: T) -> JoinHandle<Result<R, Exception>>
where
    T: Future<Output = Result<R, Exception>> + Send + 'static,
    R: Send + Sync + 'static,
{
    let ref_id = current_action_id();
    TASK_TRACKER.spawn(async move {
        log::start_action("task", ref_id, async {
            context!(task = name, location = location);
            task.await
        })
        .await
    })
}

#[inline]
pub fn spawn<T>(task: T)
where
    T: Future<Output = Result<(), Exception>> + Send + 'static,
{
    TASK_TRACKER.spawn(async {
        if let Err(err) = task.await {
            err.log();
        }
    });
}

pub async fn shutdown() {
    TASK_TRACKER.close();
    TASK_TRACKER.wait().await;
    info!("tasks finished");
}
