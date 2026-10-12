use std::future::Ready;
use std::sync::Arc;

use framework::api::ErrorResponse;
use framework::exception;
use framework::exception::Exception;
use framework::exception::error_code;
use framework::log::Severity;
use framework::web::request::Request;
use framework::web::response::Response;
use framework::web::router::Router;
use framework_macro::integration_test;
use http_test::GreetRequest;
use http_test::GreetResponse;
use http_test::TestServer;
use reqwest::StatusCode;
use reqwest::header;
use reqwest::header::HeaderValue;

struct AppState {
    prefix: &'static str,
}

async fn greet_query(state: Arc<AppState>, request: Request) -> Result<Response, Exception> {
    let request: GreetRequest = request.query()?;
    Response::json(&GreetResponse { greeting: format!("{}, {}", state.prefix, request.name) })
}

async fn greet_json(state: Arc<AppState>, mut request: Request) -> Result<Response, Exception> {
    let request: GreetRequest = request.json().await?;
    Response::json(&GreetResponse { greeting: format!("{}, {}", state.prefix, request.name) })
}

async fn echo(_state: Arc<AppState>, mut request: Request) -> Result<Response, Exception> {
    Ok(Response::text(request.text().await?))
}

async fn whoami(_state: Arc<AppState>, request: Request) -> Result<Response, Exception> {
    let session = request.cookie("session").unwrap_or_default();
    let agent = request.user_agent().unwrap_or_default();
    Ok(Response::text(format!("{}|{session}|{agent}", request.client_ip())))
}

async fn fail(_state: Arc<AppState>, _request: Request) -> Result<Response, Exception> {
    Err(exception!("expected failure", severity = Severity::Warn, code = "TEST_FAILURE"))
}

async fn panic(_state: Arc<AppState>, _request: Request) -> Result<Response, Exception> {
    panic!("expected panic")
}

// panics before returning the future
fn panic_sync(_state: Arc<AppState>, _request: Request) -> Ready<Result<Response, Exception>> {
    panic!("expected panic")
}

async fn ping(_state: Arc<AppState>, _request: Request) -> Result<Response, Exception> {
    Ok(Response::empty())
}

async fn no_content(_state: Arc<AppState>, _request: Request) -> Result<Response, Exception> {
    Ok(Response::text("ignored").status(StatusCode::NO_CONTENT))
}

#[integration_test]
async fn request_response() -> Result<(), Exception> {
    let router = Router::new()
        .merge(
            Router::new()
                .state(Arc::new(AppState { prefix: "hello" }))
                .get("/greet", greet_query)
                .post("/greet", greet_json)
                .post("/echo", echo)
                .get("/whoami", whoami)
                .get("/fail", fail)
                .get("/panic", panic)
                .get("/panic-sync", panic_sync)
                .get("/no-content", no_content),
        )
        .merge(Router::new().state(Arc::new(AppState { prefix: "unused" })).get("/ping", ping));
    let mut server = TestServer::start(router).await;
    let client = &server.http1;

    let response = client.get(server.url("/greet?name=world+%26+friends")).send().await?;
    assert_eq!(response.status(), 200);
    assert_eq!(response.version(), reqwest::Version::HTTP_11);
    assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
    let body: GreetResponse = response.json().await?;
    assert_eq!(body.greeting, "hello, world & friends");

    // a non text `client` header is skipped in the context, the request still reaches the handler
    let response = client
        .get(server.url("/greet?name=world"))
        .header("client", HeaderValue::from_bytes("café".as_bytes()).expect("header value must be valid"))
        .send()
        .await?;
    let body: GreetResponse = response.json().await?;
    assert_eq!(body.greeting, "hello, world");

    let response = client.post(server.url("/greet")).json(&GreetRequest { name: "世界".to_owned() }).send().await?;
    let body: GreetResponse = response.json().await?;
    assert_eq!(body.greeting, "hello, 世界");

    let response = client.post(server.url("/echo")).body("hello\n世界").send().await?;
    assert_eq!(response.headers()[header::CONTENT_TYPE], "text/plain; charset=utf-8");
    assert_eq!(response.text().await?, "hello\n世界");

    let response = client
        .get(server.url("/whoami"))
        .header(header::COOKIE, "a=1; session=abc%20def")
        .header(header::USER_AGENT, "test-agent")
        .header("x-forwarded-for", "108.0.0.1, 10.0.0.1")
        .send()
        .await?;
    assert_eq!(response.text().await?, "108.0.0.1|abc def|test-agent");

    let response = client.get(server.url("/ping")).send().await?;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = client.head(server.url("/greet?name=world")).send().await?;
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()[header::CONTENT_LENGTH], r#"{"greeting":"hello, world"}"#.len().to_string());

    for (path, body, message) in [
        ("/greet", Some("{"), "failed to parse json body"),
        ("/greet", Some(r#"{"name":42}"#), "failed to parse json body"),
        ("/echo", Some(&*"x".repeat(2048)), "failed to read body, error=length limit exceeded"),
    ] {
        let response = client.post(server.url(path)).body(body.unwrap_or_default().to_owned()).send().await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let error: ErrorResponse = response.json().await?;
        assert_eq!(error.code.as_deref(), Some(error_code::BAD_REQUEST));
        assert_eq!(error.severity, Severity::Warn);
        assert_eq!(error.message, message);
    }

    let response = client.get(server.url("/greet")).send().await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let error: ErrorResponse = response.json().await?;
    assert_eq!(error.message, "failed to parse query");

    let response = client.get(server.url("/fail")).send().await?;
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let error: ErrorResponse = response.json().await?;
    assert_eq!(error.code.as_deref(), Some("TEST_FAILURE"));
    assert_eq!(error.message, "expected failure");

    for path in ["/panic", "/panic-sync"] {
        let response = client.get(server.url(path)).send().await?;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR, "path={path}");
        let error: ErrorResponse = response.json().await?;
        assert_eq!(error.message, "handler panicked, error=expected panic");
    }

    for client in [&server.http1, &server.h2c] {
        let response = client.get(server.url("/no-content")).send().await?;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert!(response.headers().get(header::CONTENT_LENGTH).is_none());
        assert_eq!(response.bytes().await?.len(), 0);
    }

    let response = client.get(server.url("/unknown")).send().await?;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(response.text().await?, "");

    let response = client.put(server.url("/greet")).send().await?;
    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert!(response.headers().get(header::ALLOW).is_none());

    server.shutdown().await;
    assert!(server.http1.get(server.url("/health-check")).send().await.is_err(), "request succeeded after shutdown");
    Ok(())
}
