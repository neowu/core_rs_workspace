use axum::Router;
use axum::http::header;
use framework::api::ErrorResponse;
use framework::exception::Exception;
use framework::exception::error_code;
use framework::http::Method;
use framework::json;
use framework::log::Severity;
use framework::web::body::Json;
use framework::web::body::Query;
use framework::web::body::TextBody;
use framework::web::route::get;
use framework::web::route::post;
use framework_macro::integration_test;
use http_test::GreetRequest;
use http_test::GreetResponse;
use http_test::TestServer;

async fn greet_query(Query(request): Query<GreetRequest>) -> Json<GreetResponse> {
    Json(GreetResponse { greeting: format!("hello, {}", request.name) })
}

async fn greet_json(Json(request): Json<GreetRequest>) -> Json<GreetResponse> {
    Json(GreetResponse { greeting: format!("hello, {}", request.name) })
}

async fn echo(TextBody(body): TextBody) -> String {
    body
}

#[integration_test]
async fn request_response() -> Result<(), Exception> {
    let router = Router::new().route("/greet", get(greet_query).post(greet_json)).route("/echo", post(echo));
    let mut server = TestServer::start(router).await?;

    let response = server.client.execute(server.request(Method::GET, "/greet?name=world+%26+friends")).await?;
    assert_eq!(response.status, 200);
    assert_eq!(response.headers.get(&header::CONTENT_TYPE).map(String::as_str), Some("application/json"));
    let body: GreetResponse = json::from_json(&response.body)?;
    assert_eq!(body.greeting, "hello, world & friends");

    let mut request = server.request(Method::POST, "/greet");
    request.body(json::to_json(&GreetRequest { name: "世界".to_owned() })?, "application/json");
    let response = server.client.execute(request).await?;
    assert_eq!(response.status, 200);
    let body: GreetResponse = json::from_json(&response.body)?;
    assert_eq!(body.greeting, "hello, 世界");

    let mut request = server.request(Method::POST, "/echo");
    request.body("hello\n世界".to_owned(), "text/plain");
    let response = server.client.execute(request).await?;
    assert_eq!(response.status, 200);
    assert_eq!(response.body, "hello\n世界");
    assert_eq!(response.headers.get(&header::CONTENT_TYPE).map(String::as_str), Some("text/plain; charset=utf-8"));

    for (method, path, body, message) in [
        (Method::GET, "/greet", None, "failed to parse query"),
        (Method::POST, "/greet", Some("{"), "failed to parse json body"),
        (Method::POST, "/greet", Some(r#"{"name":42}"#), "failed to parse json body"),
    ] {
        let mut request = server.request(method, path);
        if let Some(body) = body {
            request.body(body.to_owned(), "application/json");
        }
        let response = server.client.execute(request).await?;
        assert_eq!(response.status, 400);
        let error: ErrorResponse = json::from_json(&response.body)?;
        assert_eq!(error.code.as_deref(), Some(error_code::BAD_REQUEST));
        assert_eq!(error.severity, Severity::Warn);
        assert_eq!(error.message, message);
    }

    for (method, path, status) in [(Method::GET, "/unknown", 404), (Method::PUT, "/greet", 405)] {
        let response = server.client.execute(server.request(method, path)).await?;
        assert_eq!(response.status, status);
    }

    server.shutdown().await;
    let Err(error) = server.client.execute(server.request(Method::GET, "/health-check")).await else {
        panic!("request succeeded after server shutdown");
    };
    assert_eq!(error.code, Some("HTTP_REQUEST_FAILED"));
    Ok(())
}
