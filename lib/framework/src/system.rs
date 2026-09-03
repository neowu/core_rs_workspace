use std::panic;
use std::sync::OnceLock;

use futures::future::join_all;
use tokio::signal;
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;
pub use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::appender::Appender;
use crate::appender::Message;
use crate::metrics::Metrics;
use crate::metrics::MetricsCollector;
use crate::network::hostname;

pub struct System<S = Init> {
    token: CancellationToken,
    tracker: TaskTracker,
    daemon_token: CancellationToken,
    state: S,
}

/// Setup phase, metrics can be added, nothing is running yet.
pub struct Init {
    // created on the first add_metrics, moved into the collector daemon by start_appender
    collector: Option<MetricsCollector>,
}

/// Running phase, the appender daemon owns the channel, services can start.
pub struct Running {
    daemon_handles: Vec<JoinHandle<()>>,
}

pub(crate) struct Context {
    pub app: &'static str,
    pub host: String,
}

pub(crate) static CONTEXT: OnceLock<Context> = OnceLock::new();
pub(crate) static SENDER: OnceLock<UnboundedSender<Message>> = OnceLock::new();

/// Resolves the host name from the runtime env, on managed platforms the os hostname carries no
/// information, e.g. gcloud run always reports "localhost".
/// An env must not fail the startup, fall back to [hostname] when it cannot resolve anything.
pub trait Env {
    fn host(&self) -> impl Future<Output = String> + Send;
}

/// Default env, uses the os hostname.
pub struct DefaultEnv;

impl Env for DefaultEnv {
    async fn host(&self) -> String {
        hostname()
    }
}

impl System<Init> {
    pub async fn init(app: &'static str, env: impl Env) -> Self {
        let host = env.host().await;

        let token = CancellationToken::new();
        let _result = CONTEXT.set(Context { app, host });

        listen_shutdown_signal(token.clone());

        System {
            token,
            tracker: TaskTracker::new(),
            daemon_token: CancellationToken::new(),
            state: Init { collector: None },
        }
    }

    pub fn add_metrics(&mut self, metrics: impl Fn(&mut Metrics) + Send + 'static) {
        self.state.collector.get_or_insert_with(MetricsCollector::new).add(metrics);
    }

    pub fn start_logger(self, appender: impl Appender) -> System<Running> {
        let System { token, tracker, daemon_token, state: Init { collector } } = self;

        let mut daemon_handles = Vec::new();

        let (sender, mut receiver) = mpsc::unbounded_channel();
        SENDER.set(sender.clone()).unwrap_or_else(|_| panic!("appender can only start once"));

        // the collector is the only other sender, nothing is spawned when no metrics were added
        if let Some(collector) = collector {
            daemon_handles.push(tokio::spawn(collector.start(daemon_token.clone(), sender)));
        }

        let appender_token = daemon_token.clone();
        daemon_handles.push(tokio::spawn(async move {
            let mut draining = false;
            loop {
                tokio::select! {
                    // on shutdown, close the channel and keep looping to drain what is left
                    () = appender_token.cancelled(), if !draining => {
                        draining = true;
                        receiver.close();
                    }
                    Some(message) = receiver.recv() => match message {
                        Message::Action(action) => appender.append_action(action).await,
                        Message::Metrics(metrics) => appender.append_metrics(metrics).await,
                    },
                    else => break,
                }
            }
            console!("appender stopped");
        }));

        System { token, tracker, daemon_token, state: Running { daemon_handles } }
    }
}

impl System<Running> {
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

    pub async fn shutdown_logger(self) {
        self.daemon_token.cancel();
        join_all(self.state.daemon_handles).await;
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

        #[cfg(not(unix))]
        let terminate = std::future::pending::<()>();

        tokio::select! {
            () = ctrl_c => {},
            () = terminate => {},
        }

        console!("received shutdown signal");
        token.cancel();
    });
}
