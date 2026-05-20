use std::future::Future;
use std::sync::LazyLock;

use tokio::task::JoinHandle;
use tokio_util::task::TaskTracker;
use tracing::Instrument as _;
use tracing::Span;
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
pub fn __spawn_action<T>(name: &'static str, location: &'static str, task: T)
where
    T: Future<Output = Result<(), Exception>> + Send + 'static,
{
    let ref_id = current_action_id();
    TASK_TRACKER.spawn(async move {
        log::start_action("task", ref_id, async {
            context!(task = name, location = location);
            task.await
        })
        .await;
    });
}

pub fn spawn_task<T>(task: T) -> JoinHandle<Result<(), Exception>>
where
    T: Future<Output = Result<(), Exception>> + Send + 'static,
{
    let span = Span::current();
    TASK_TRACKER.spawn(task.instrument(span))
}

pub async fn shutdown() {
    TASK_TRACKER.close();
    TASK_TRACKER.wait().await;
    info!("tasks finished");
}
