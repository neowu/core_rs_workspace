use std::panic;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::OnceLock;
use std::time::Duration;

use futures::future::join_all;
use tokio::signal;
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;
pub use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::exception::Exception;
use crate::log;
use crate::log::ActionAppender;
use crate::log::ActionMessage;
use crate::metrics::Counter;
use crate::metrics::Metrics;
use crate::metrics::MetricsAppender;
use crate::metrics::MetricsCollector;
use crate::network::hostname;
use crate::task::TaskExecutor;

pub struct System {
    token: CancellationToken,
    tracker: TaskTracker,

    daemon_token: CancellationToken,
    daemon_handles: Vec<JoinHandle<()>>,
}

pub(crate) struct Context {
    pub app: &'static str,
    pub host: String,
}

pub(crate) static CONTEXT: OnceLock<Context> = OnceLock::new();
pub(crate) static ACTION_SENDER: OnceLock<UnboundedSender<ActionMessage>> = OnceLock::new();
static EXECUTOR: LazyLock<Executor> = LazyLock::new(Executor::default);

impl System {
    pub fn init(app: &'static str) -> Self {
        let token = CancellationToken::new();
        let _result = CONTEXT.set(Context { app, host: hostname() });

        listen_shutdown_signal(token.clone());

        System { token, tracker: TaskTracker::new(), daemon_token: CancellationToken::new(), daemon_handles: vec![] }
    }

    pub fn start_action_logger(&mut self, appender: impl ActionAppender) {
        let token = self.daemon_token.clone();

        let (sender, mut receiver) = mpsc::unbounded_channel();
        ACTION_SENDER.set(sender).unwrap_or_else(|_| panic!("action logger can only start once"));

        self.daemon_handles.push(tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = token.cancelled() => break,
                    Some(message) = receiver.recv() => appender.append(message).await,
                    else => break,
                }
            }
            receiver.close();
            while let Some(message) = receiver.recv().await {
                // drain what's left
                appender.append(message).await;
            }
            console!("action appender stopped");
        }));
    }

    pub fn start_metrics_collector(&mut self, collector: MetricsCollector, appender: impl MetricsAppender) {
        let token = self.daemon_token.clone();

        let (sender, mut receiver) = mpsc::unbounded_channel();

        self.daemon_handles.push(tokio::spawn(collector.start(token.clone(), sender)));

        self.daemon_handles.push(tokio::spawn(async move {
            // only MetricsCollector has sender, which will be dropped on shutdown
            while let Some(message) = receiver.recv().await {
                appender.append(message).await;
            }
            console!("metrics appender stopped");
        }));
    }

    pub fn start_service<T, F>(&self, task: T)
    where
        T: FnOnce(CancellationToken) -> F,
        F: Future<Output = ()> + Send + 'static,
    {
        self.tracker.spawn(task(self.token.clone()));
    }

    pub async fn wait(&self) {
        self.tracker.close();
        self.tracker.wait().await;
    }

    pub async fn shutdown(self, timeout: Duration) -> Result<(), Exception> {
        EXECUTOR.shutdown(timeout).await;

        self.daemon_token.cancel();
        join_all(self.daemon_handles).await;
        console!("system daemon stopped");

        Ok(())
    }

    pub fn executor_metrics(&self) -> impl Fn(&mut Metrics) + Send + 'static {
        EXECUTOR.metrics()
    }
}

pub fn app() -> Option<&'static str> {
    CONTEXT.get().map(|context| context.app)
}

fn listen_shutdown_signal(token: CancellationToken) {
    tokio::spawn(async move {
        let ctrl_c = async {
            signal::ctrl_c().await.expect("failed to listen ctrl+c");
        };

        #[cfg(unix)]
        let terminate = async {
            use tokio::signal::unix::SignalKind;

            signal::unix::signal(SignalKind::terminate()).expect("failed to listen signal").recv().await;
        };

        tokio::select! {
            () = ctrl_c => {},
            () = terminate => {},
        }

        console!("received shutdown signal");
        token.cancel();
    });
}

#[doc(hidden)]
pub fn __spawn_action<T, R>(name: &'static str, location: &'static str, task: T)
where
    T: Future<Output = Result<R, Exception>> + Send + 'static,
    R: Send + Sync + 'static,
{
    EXECUTOR.spawn(name, location, task);
}

#[macro_export]
macro_rules! spawn_action {
    ($name:expr, $task:expr) => {
        $crate::system::__spawn_action($name, concat!(file!(), ":", line!()), $task)
    };
}

#[derive(Default)]
struct Executor {
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

    fn metrics(&self) -> impl Fn(&mut Metrics) + Send + 'static {
        let counter = Arc::clone(&self.counter);
        move |metrics| {
            metrics.stats.push(("active_tasks", counter.max() as u64));
        }
    }

    async fn shutdown(&self, timeout: Duration) {
        if let Some(aborted) = self.executor.shutdown(timeout).await {
            console!("WARN tasks aborted, tasks={aborted:?}");
        } else {
            console!("tasks finished");
        }
    }
}
