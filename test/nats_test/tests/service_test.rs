use std::sync::Arc;
use std::time::Duration;

use framework::exception;
use framework::exception::Exception;
use framework::log;
use framework::log::Severity;
use framework::system::CancellationToken;
use framework_macro::integration_test;
use framework_macro::nats_api;
use framework_nats::service::ServiceClient;
use framework_nats::service::ServiceConfig;
use nats_test::client;
use serde::Deserialize;
use serde::Serialize;

const UNKNOWN: &str = "api.nats_test.unknown";

#[derive(Serialize, Deserialize, Debug)]
struct GreetRequest {
    name: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct GreetResponse {
    greeting: String,
}

#[nats_api]
trait GreetingService {
    #[subject = "api.nats_test.greet"]
    async fn greet(&self, request: GreetRequest) -> Result<GreetResponse, Exception>;

    #[subject = "api.nats_test.ping"]
    async fn ping(&self) -> Result<(), Exception>;

    #[subject = "api.nats_test.fail"]
    async fn fail(&self) -> Result<(), Exception>;
}

struct GreetingServiceImpl;

impl GreetingService for GreetingServiceImpl {
    async fn greet(&self, request: GreetRequest) -> Result<GreetResponse, Exception> {
        log::trace();
        Ok(GreetResponse { greeting: format!("hello, {}", request.name) })
    }

    async fn ping(&self) -> Result<(), Exception> {
        Ok(())
    }

    async fn fail(&self) -> Result<(), Exception> {
        Err(exception!("expected failure", severity = Severity::Warn, code = "TEST_FAILURE"))
    }
}

#[integration_test]
async fn service() -> Result<(), Exception> {
    let nats_client = client().await;

    // the test drives shutdown itself, System only cancels on a signal
    let shutdown_signal = CancellationToken::new();
    let service =
        GreetingService::service(nats_client.clone(), Arc::new(GreetingServiceImpl), ServiceConfig::default());
    let service = tokio::spawn(service.start(shutdown_signal.clone()));

    let client = Arc::new(GreetingServiceClient::new(nats_client.clone()));
    wait_until_started(&client).await;

    // request/response
    let response = client.greet(GreetRequest { name: "world".to_owned() }).await?;
    assert_eq!(response.greeting, "hello, world");

    // both request and response can be ()
    client.ping().await?;

    // exception is propagated back to the client, with severity and code
    let error = client.fail().await.unwrap_err();
    assert_eq!(error.severity, Severity::Warn);
    assert_eq!(error.code, Some("TEST_FAILURE"));
    assert!(error.message.contains("expected failure"), "message={}", error.message);

    let service_client = ServiceClient::new(nats_client.clone());

    // request the service fails to decode
    let error = service_client.request::<i32, GreetResponse>("api.nats_test.greet", &1).await.unwrap_err();
    assert_eq!(error.code, Some("NATS_INVALID_MESSAGE"));

    // subject without service
    let error = service_client.request::<(), ()>(UNKNOWN, &()).await.unwrap_err();
    assert_eq!(error.code, Some("NATS_NO_RESPONDERS"));

    // service unsubscribes on shutdown, requests are rejected right away
    shutdown_signal.cancel();
    service.await.unwrap();

    let error = client.ping().await.unwrap_err();
    assert_eq!(error.code, Some("NATS_NO_RESPONDERS"));

    Ok(())
}

// the service subscribes after start() is spawned, requests before that get no responders
async fn wait_until_started(client: &GreetingServiceClient) {
    for _ in 0..100 {
        if client.ping().await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("service did not start");
}
