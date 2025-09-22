use std::future::Future;
use std::sync::LazyLock;

use tokio::task::JoinHandle;
use tokio_util::task::TaskTracker;
use tracing::Instrument;
use tracing::Span;
use tracing::debug;
use tracing::info;

use crate::exception::CoreRsResult;
use crate::log;
use crate::log::current_action_id;

static TASK_TRACKER: LazyLock<TaskTracker> = LazyLock::new(TaskTracker::new);

pub fn spawn_action<T>(name: &'static str, task: T)
where
    T: Future<Output = CoreRsResult<()>> + Send + 'static,
{
    let ref_id = current_action_id();
    TASK_TRACKER.spawn(async move {
        log::start_action("task", ref_id, async {
            debug!(task = name, "context");
            task.await
        })
        .await
    });
}

pub fn spawn_task<T>(task: T) -> JoinHandle<CoreRsResult<()>>
where
    T: Future<Output = CoreRsResult<()>> + Send + 'static,
{
    let span = Span::current();
    TASK_TRACKER.spawn(task.instrument(span))
}

pub async fn shutdown() {
    info!("waiting for {} task(s) to finish", TASK_TRACKER.len());
    TASK_TRACKER.close();
    TASK_TRACKER.wait().await;
    info!("tasks finished");
}
