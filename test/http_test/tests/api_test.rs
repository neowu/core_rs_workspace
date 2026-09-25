use std::sync::Arc;

use axum::http::header;
use framework::api::ErrorResponse;
use framework::exception;
use framework::exception::Exception;
use framework::exception::error_code;
use framework::http::Method;
use framework::json;
use framework::log::Severity;
use framework_macro::api;
use framework_macro::integration_test;
use http_test::GreetRequest;
use http_test::GreetResponse;
use http_test::TestServer;

#[api]
trait GreetingService {
    #[get]
    #[path("/api/greet")]
    async fn greet(&self, request: GreetRequest) -> Result<GreetResponse, Exception>;

    #[post]
    #[path("/api/greet")]
    async fn create(&self, request: GreetRequest) -> Result<GreetResponse, Exception>;

    #[put]
    #[path("/api/greet")]
    async fn update(&self, request: GreetRequest) -> Result<GreetResponse, Exception>;

    #[get]
    #[path("/api/ping")]
    async fn ping(&self) -> Result<(), Exception>;

    #[post]
    #[path("/api/fail")]
    async fn fail(&self) -> Result<(), Exception>;
}

struct GreetingServiceImpl;

impl GreetingService for GreetingServiceImpl {
    async fn greet(&self, request: GreetRequest) -> Result<GreetResponse, Exception> {
        Ok(GreetResponse { greeting: format!("hello, {}", request.name) })
    }

    async fn create(&self, request: GreetRequest) -> Result<GreetResponse, Exception> {
        Ok(GreetResponse { greeting: format!("created, {}", request.name) })
    }

    async fn update(&self, request: GreetRequest) -> Result<GreetResponse, Exception> {
        if request.name.is_empty() {
            return Err(exception!("name is required", severity = Severity::Warn, code = error_code::VALIDATION_ERROR));
        }
        Ok(GreetResponse { greeting: format!("updated, {}", request.name) })
    }

    async fn ping(&self) -> Result<(), Exception> {
        Ok(())
    }

    async fn fail(&self) -> Result<(), Exception> {
        Err(exception!("expected failure", severity = Severity::Warn, code = "TEST_FAILURE"))
    }
}

#[integration_test]
async fn api() -> Result<(), Exception> {
    let mut server = TestServer::start(GreetingService::route(Arc::new(GreetingServiceImpl))).await?;
    let client = GreetingServiceClient::new(server.client.clone(), server.url.clone());

    let response = client.greet(GreetRequest { name: "world & 世界 + ?".to_owned() }).await?;
    assert_eq!(response.greeting, "hello, world & 世界 + ?");
    let response = client.create(GreetRequest { name: "world".to_owned() }).await?;
    assert_eq!(response.greeting, "created, world");
    let response = client.update(GreetRequest { name: "world".to_owned() }).await?;
    assert_eq!(response.greeting, "updated, world");
    client.ping().await?;

    let response = server.client.execute(server.request(Method::GET, "/api/ping")).await?;
    assert_eq!(response.status, 204);
    assert!(response.body.is_empty(), "unit response must have an empty body");

    let error = client.update(GreetRequest { name: String::new() }).await.unwrap_err();
    assert_eq!(error.severity, Severity::Warn);
    assert_eq!(error.code, Some(error_code::VALIDATION_ERROR));
    assert!(error.message.contains("status=400"), "message={}", error.message);
    assert!(error.message.contains("name is required"), "message={}", error.message);

    let error = client.fail().await.unwrap_err();
    assert_eq!(error.severity, Severity::Warn);
    assert_eq!(error.code, Some("TEST_FAILURE"));
    assert!(error.message.contains("status=500"), "message={}", error.message);
    assert!(error.message.contains("expected failure"), "message={}", error.message);

    let response = server.client.execute(server.request(Method::POST, "/api/fail")).await?;
    assert_eq!(response.status, 500);
    assert_eq!(response.headers.get(&header::CONTENT_TYPE).map(String::as_str), Some("application/json"));
    let error: ErrorResponse = json::from_json(&response.body)?;
    assert_eq!(error.severity, Severity::Warn);
    assert_eq!(error.code.as_deref(), Some("TEST_FAILURE"));
    assert_eq!(error.message, "expected failure");

    let missing_client = GreetingServiceClient::new(server.client.clone(), format!("{}/unknown", server.url));
    let error = missing_client.ping().await.unwrap_err();
    assert!(error.message.contains("status=404"), "message={}", error.message);

    server.shutdown().await;
    let error = client.ping().await.unwrap_err();
    assert_eq!(error.code, Some("HTTP_REQUEST_FAILED"));
    Ok(())
}
