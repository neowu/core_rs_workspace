use std::sync::Arc;
use std::time::Duration;

use framework::exception::Exception;
use framework::system::CancellationToken;
use framework_macro::integration_test;
use framework_macro::nats_api;
use framework_nats::service::ServiceConfig;
use nats_test::client;
use tokio::time::sleep;
use tokio::time::timeout;

#[nats_api]
trait DrainService {
    #[subject = "api.nats_test.drain.ping"]
    async fn ping(&self) -> Result<(), Exception>;

    #[subject = "api.nats_test.drain.slow"]
    async fn slow(&self) -> Result<(), Exception>;
}

struct DrainServiceImpl {
    release: CancellationToken,
}

impl DrainService for DrainServiceImpl {
    async fn ping(&self) -> Result<(), Exception> {
        Ok(())
    }

    async fn slow(&self) -> Result<(), Exception> {
        self.release.cancelled().await;
        Ok(())
    }
}

// a saturated service buffers delivered requests in the client, and core nats never redelivers them:
// shutdown must still handle them, not drop them for the callers to time out
#[integration_test]
async fn service_drain() -> Result<(), Exception> {
    let nats_client = client().await;

    let release = CancellationToken::new();
    let shutdown_signal = CancellationToken::new();
    let service = DrainService::service(
        nats_client.clone(),
        Arc::new(DrainServiceImpl { release: release.clone() }),
        ServiceConfig { max_concurrency: 1 },
    );
    let service = tokio::spawn(service.start(shutdown_signal.clone()));

    let client = Arc::new(DrainServiceClient::new(nats_client));
    wait_until_started(&client).await;

    // the first takes the only permit, the rest are buffered behind it
    let requests: Vec<_> = (0..5)
        .map(|_| {
            let client = Arc::clone(&client);
            tokio::spawn(async move { client.slow().await })
        })
        .collect();
    sleep(Duration::from_millis(300)).await;

    shutdown_signal.cancel();
    release.cancel();

    for request in requests {
        let result = timeout(Duration::from_secs(5), request).await.expect("request must be answered").unwrap();
        assert!(result.is_ok(), "buffered request must be handled on shutdown, error={result:?}");
    }
    timeout(Duration::from_secs(5), service).await.expect("service must stop once drained").unwrap();

    Ok(())
}

async fn wait_until_started(client: &DrainServiceClient) {
    for _ in 0..100 {
        if client.ping().await.is_ok() {
            return;
        }
        sleep(Duration::from_millis(20)).await;
    }
    panic!("service did not start");
}
