use std::sync::Arc;
use std::time::Duration;

use framework::exception::Exception;
use framework::system::CancellationToken;
use framework_macro::integration_test;
use framework_macro::nats_api;
use framework_nats::service::ServiceConfig;
use nats_test::client;
use tokio::sync::Notify;
use tokio::time::sleep;
use tokio::time::timeout;

#[nats_api]
trait ShutdownService {
    #[subject = "api.nats_test.shutdown.ping"]
    async fn ping(&self) -> Result<(), Exception>;

    #[subject = "api.nats_test.shutdown.stuck"]
    async fn stuck(&self) -> Result<(), Exception>;
}

struct ShutdownServiceImpl {
    started: Arc<Notify>,
    release: CancellationToken,
}

impl ShutdownService for ShutdownServiceImpl {
    async fn ping(&self) -> Result<(), Exception> {
        Ok(())
    }

    async fn stuck(&self) -> Result<(), Exception> {
        self.started.notify_one();
        self.release.cancelled().await;
        Ok(())
    }
}

// every permit held by a handler that does not finish, and another request waiting: shutdown must
// still stop pulling and unsubscribe, not wait for a permit that never comes
#[integration_test]
async fn service_shutdown() -> Result<(), Exception> {
    let nats_client = client().await;

    let started = Arc::new(Notify::new());
    let release = CancellationToken::new();
    let shutdown_signal = CancellationToken::new();
    let service = ShutdownService::service(
        nats_client.clone(),
        Arc::new(ShutdownServiceImpl { started: Arc::clone(&started), release: release.clone() }),
        ServiceConfig { max_concurrency: 1 },
    );
    let service = tokio::spawn(service.start(shutdown_signal.clone()));

    let client = Arc::new(ShutdownServiceClient::new(nats_client.clone()));
    wait_until_started(&client).await;

    // the first takes the only permit, the second queues behind it (before or after shutdown, either way
    // the service must not wait for its permit)
    for _ in 0..2 {
        let client = Arc::clone(&client);
        tokio::spawn(async move { client.stuck().await });
    }
    started.notified().await;

    shutdown_signal.cancel();
    wait_until_unsubscribed(&client).await;

    // the in-flight handler is still drained, the service stops once it finishes
    release.cancel();
    timeout(Duration::from_secs(5), service).await.expect("service must stop once handlers finish").unwrap();

    Ok(())
}

// the service subscribes after start() is spawned, requests before that get no responders
async fn wait_until_started(client: &ShutdownServiceClient) {
    for _ in 0..100 {
        if client.ping().await.is_ok() {
            return;
        }
        sleep(Duration::from_millis(20)).await;
    }
    panic!("service did not start");
}

// unsubscribe is async (async-nats sends UNSUB from a spawned task); until the server drops the
// subscription a ping is routed to the saturated service and never answered, so each attempt is bounded
async fn wait_until_unsubscribed(client: &ShutdownServiceClient) {
    for _ in 0..100 {
        if let Ok(Err(e)) = timeout(Duration::from_millis(20), client.ping()).await
            && e.code == Some("NATS_NO_RESPONDERS")
        {
            return;
        }
    }
    panic!("service must unsubscribe on shutdown while saturated");
}
