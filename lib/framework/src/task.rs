use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::time;
use tokio_util::task::TaskTracker;

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
