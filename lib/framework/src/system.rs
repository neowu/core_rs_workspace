use tokio::signal;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::info;

pub struct System {
    token: CancellationToken,
    handles: JoinSet<()>,
}

impl System {
    pub fn new() -> Self {
        let token = CancellationToken::new();
        let shutdown_signal = token.clone();
        tokio::spawn(async move {
            let ctrl_c = async {
                signal::ctrl_c().await.expect("failed to install Ctrl+C handler");
            };

            #[cfg(unix)]
            let terminate = async {
                use tokio::signal::unix::SignalKind;

                signal::unix::signal(SignalKind::terminate()).expect("failed to install signal handler").recv().await;
            };

            tokio::select! {
                () = ctrl_c => {},
                () = terminate => {},
            }

            info!("received shutdown signal");
            shutdown_signal.cancel();
        });
        Self { token, handles: JoinSet::new() }
    }

    pub fn shutdown_signal(&self) -> CancellationToken {
        self.token.clone()
    }

    pub fn spawn<F>(&mut self, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.handles.spawn(task);
    }

    pub async fn wait(self) {
        self.handles.join_all().await;
    }
}

impl Default for System {
    fn default() -> Self {
        Self::new()
    }
}
