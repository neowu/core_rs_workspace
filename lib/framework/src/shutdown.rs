use tokio::signal;
use tokio::signal::unix::SignalKind;
use tokio_util::sync::CancellationToken;
use tracing::info;

pub fn listen_shutdown_signal() -> CancellationToken {
    let token = CancellationToken::new();
    let cancel_token = token.clone();
    tokio::spawn(async move {
        let ctrl_c = async {
            signal::ctrl_c().await.expect("failed to install Ctrl+C handler");
        };

        #[cfg(unix)]
        let terminate = async {
            signal::unix::signal(SignalKind::terminate()).expect("failed to install signal handler").recv().await;
        };

        tokio::select! {
            () = ctrl_c => {},
            () = terminate => {},
        }

        info!("received shutdown signal");
        cancel_token.cancel();
    });
    token
}
