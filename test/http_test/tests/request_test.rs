use std::sync::Arc;

use framework::api::ErrorResponse;
use framework::exception;
use framework::exception::Exception;
use framework::exception::error_code;
use framework::http::HeaderName;
use framework::http::HeaderValue;
use framework::http::HttpRequest;
use framework::http::Method;
use framework::http::StatusCode;
use framework::http::header;
use framework::json;
use framework::log::Severity;
use framework::web::request::Request;
use framework::web::response::Response;
use framework::web::router::Router;
use framework_macro::integration_test;
use http_test::GreetRequest;
use http_test::GreetResponse;
use http_test::TestServer;
use http_test::execute;
use http_test::http_client;

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
                .route(Method::GET, "/greet", greet_query)
                .route(Method::POST, "/greet", greet_json)
                .route(Method::POST, "/echo", echo)
                .route(Method::GET, "/whoami", whoami)
                .route(Method::GET, "/fail", fail)
                .route(Method::GET, "/panic", panic)
                .route(Method::GET, "/no-content", no_content),
        )
        .merge(Router::new().state(Arc::new(AppState { prefix: "unused" })).route(Method::GET, "/ping", ping));
    let mut server = TestServer::start(router).await;
    let client = &http_client(false);
    let json_request = |method, path, body: &str| {
        let mut request = HttpRequest::new(method, server.url(path))?;
        request.body(body.to_owned(), "application/json");
        Ok::<_, Exception>(request)
    };

    let response = execute(client, Method::GET, server.url("/greet?name=world+%26+friends")).await?;
    assert_eq!(response.status, 200);
    assert_eq!(response.headers[header::CONTENT_TYPE], "application/json");
    let body: GreetResponse = json::from_json(response.text()?)?;
    assert_eq!(body.greeting, "hello, world & friends");

    // a non text `client` header is skipped in the context, the request still reaches the handler
    let mut request = HttpRequest::new(Method::GET, server.url("/greet?name=world"))?;
    request.headers.insert(
        HeaderName::from_static("client"),
        HeaderValue::from_bytes("café".as_bytes()).expect("header value must be valid"),
    );
    let body: GreetResponse = json::from_json(client.execute(request).await?.text()?)?;
    assert_eq!(body.greeting, "hello, world");

    let request = json_request(Method::POST, "/greet", &json::to_json(&GreetRequest { name: "世界".to_owned() })?)?;
    let body: GreetResponse = json::from_json(client.execute(request).await?.text()?)?;
    assert_eq!(body.greeting, "hello, 世界");

    let mut request = HttpRequest::new(Method::POST, server.url("/echo"))?;
    request.body("hello\n世界".to_owned(), "text/plain");
    let response = client.execute(request).await?;
    assert_eq!(response.headers[header::CONTENT_TYPE], "text/plain; charset=utf-8");
    assert_eq!(response.text()?, "hello\n世界");

    let mut request = HttpRequest::new(Method::GET, server.url("/whoami"))?;
    request.header(header::COOKIE, "a=1; session=abc%20def")?;
    request.header(header::USER_AGENT, "test-agent")?;
    request.header(HeaderName::from_static("x-forwarded-for"), "108.0.0.1, 10.0.0.1")?;
    assert_eq!(client.execute(request).await?.text()?, "108.0.0.1|abc def|test-agent");

    let mut request = HttpRequest::new(Method::GET, server.url("/whoami"))?;
    let forwarded_for = HeaderName::from_static("x-forwarded-for");
    request.headers.append(&forwarded_for, HeaderValue::from_static("1.1.1.1"));
    request.headers.append(&forwarded_for, HeaderValue::from_static("108.0.0.1, 10.0.0.1"));
    let response = client.execute(request).await?;
    assert_eq!(response.status, 400);
    let error: ErrorResponse = json::from_json(response.text()?)?;
    assert_eq!(error.message, "duplicate header, name=x-forwarded-for");

    let mut request = HttpRequest::new(Method::GET, server.url("/whoami"))?;
    request.header(forwarded_for, "1.2.3, 10.0.0.1")?;
    let response = client.execute(request).await?;
    assert_eq!(response.status, 400);
    let error: ErrorResponse = json::from_json(response.text()?)?;
    assert_eq!(error.message, "invalid client ip address, value=1.2.3");

    let response = execute(client, Method::GET, server.url("/ping")).await?;
    assert_eq!(response.status, 204);

    let response = execute(client, Method::HEAD, server.url("/greet?name=world")).await?;
    assert_eq!(response.status, 200);
    assert_eq!(response.headers[header::CONTENT_LENGTH], r#"{"greeting":"hello, world"}"#.len().to_string());
    assert!(response.body.is_empty());

    for (path, body, message) in [
        ("/greet", "{", "failed to parse json body"),
        ("/greet", r#"{"name":42}"#, "failed to parse json body"),
        ("/greet", r#"{"name":"a\u0000"}"#, "json body must not contain NUL char"),
        ("/echo", &*"x".repeat(2048), "failed to read body, error=length limit exceeded"),
    ] {
        let response = client.execute(json_request(Method::POST, path, body)?).await?;
        assert_eq!(response.status, 400);
        let error: ErrorResponse = json::from_json(response.text()?)?;
        assert_eq!(error.code.as_deref(), Some(error_code::BAD_REQUEST));
        assert_eq!(error.severity, Severity::Warn);
        assert_eq!(error.message, message);
    }

    let response = execute(client, Method::GET, server.url("/greet")).await?;
    assert_eq!(response.status, 400);
    let error: ErrorResponse = json::from_json(response.text()?)?;
    assert_eq!(error.message, "failed to parse query");

    let response = execute(client, Method::GET, server.url("/fail")).await?;
    assert_eq!(response.status, 500);
    let error: ErrorResponse = json::from_json(response.text()?)?;
    assert_eq!(error.code.as_deref(), Some("TEST_FAILURE"));
    assert_eq!(error.message, "expected failure");

    let response = execute(client, Method::GET, server.url("/panic")).await?;
    assert_eq!(response.status, 500);
    let error: ErrorResponse = json::from_json(response.text()?)?;
    assert_eq!(error.message, "handler panicked, error=expected panic");

    for h2c in [false, true] {
        let client = &http_client(h2c);
        let response = execute(client, Method::GET, server.url("/no-content")).await?;
        assert_eq!(response.status, 204);
        assert!(response.headers.get(header::CONTENT_LENGTH).is_none());
        assert!(response.body.is_empty());
    }

    let response = execute(client, Method::GET, server.url("/unknown")).await?;
    assert_eq!(response.status, 404);
    assert!(response.body.is_empty());

    let response = execute(client, Method::PUT, server.url("/greet")).await?;
    assert_eq!(response.status, 405);
    assert!(response.headers.get(header::ALLOW).is_none());

    server.shutdown().await;
    let result = execute(client, Method::GET, server.url("/health-check")).await;
    assert!(result.is_err(), "request succeeded after shutdown");
    Ok(())
}
