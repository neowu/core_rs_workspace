use std::sync::Arc;

use framework::exception;
use framework::exception::Exception;
use framework::exception::error_code;
use framework::http::HeaderName;
use framework::http::HeaderValue;
use framework::http::HttpRequest;
use framework::http::Method;
use framework::log::Severity;
use framework::web::access_control;
use framework::web::request::Request;
use framework::web::response::Response;
use framework::web::router::Next;
use framework::web::router::Router;
use framework_macro::integration_test;
use http_test::TestServer;
use http_test::execute;
use http_test::http_client;

const TRACE: HeaderName = HeaderName::from_static("trace");

struct AppState {
    token: &'static str,
}

#[derive(Clone)]
struct User(String);

async fn trace(_state: Arc<AppState>, request: Request, next: Next) -> Result<Response, Exception> {
    let response = next.run(request).await?;
    Ok(response.header(TRACE, HeaderValue::from_static("traced")))
}

async fn login(state: Arc<AppState>, mut request: Request, next: Next) -> Result<Response, Exception> {
    if request.header("token") != Some(state.token) {
        return Err(exception!("login required", severity = Severity::Warn, code = error_code::FORBIDDEN));
    }
    request.set_extension(User("neo".to_owned()));
    next.run(request).await
}

async fn account(_state: Arc<AppState>, request: Request) -> Result<Response, Exception> {
    let user = request.extension::<User>().ok_or_else(|| exception!("user not found"))?;
    Ok(Response::text(user.0.clone()))
}

async fn sys_info(_state: Arc<AppState>, request: Request) -> Result<Response, Exception> {
    access_control::validate_local_peer(&request)?;
    Ok(Response::text("info"))
}

async fn public(_state: Arc<AppState>, request: Request) -> Result<Response, Exception> {
    Ok(Response::text(request.extension::<User>().is_some().to_string()))
}

#[integration_test]
async fn interceptor() -> Result<(), Exception> {
    let state = Arc::new(AppState { token: "secret" });
    let router = Router::new()
        .merge(Router::state(Arc::clone(&state)).route(Method::GET, "/public", public).route(
            Method::GET,
            "/_sys/info",
            sys_info,
        ))
        .merge(
            Router::state(state)
                .intercept(trace)
                .route(Method::GET, "/account", account)
                // applies to routes registered before it as well
                .intercept(login)
                .prefix(Method::GET, "/account/", account),
        );
    let mut server = TestServer::start(router).await;
    let client = &http_client(false);

    // interceptors only apply to their own router
    let response = execute(client, Method::GET, server.url("/public")).await?;
    assert_eq!(response.status, 200);
    assert!(!response.headers.contains_key(TRACE));
    assert_eq!(response.text()?, "false");

    for path in ["/account", "/account/detail"] {
        // rejected by `login`, the error skips the rest of `trace` and the handler
        let response = execute(client, Method::GET, server.url(path)).await?;
        assert_eq!(response.status, 403);
        assert!(!response.headers.contains_key(TRACE));

        let mut request = HttpRequest::new(Method::GET, server.url(path))?;
        request.headers.insert(HeaderName::from_static("token"), HeaderValue::from_static("secret"));
        let response = client.execute(request).await?;
        assert_eq!(response.status, 200);
        assert_eq!(response.headers[TRACE], "traced");
        assert_eq!(response.text()?, "neo");
    }

    // local only, the test client is on loopback, a non local peer address is covered by unit tests
    let response = execute(client, Method::GET, server.url("/_sys/info")).await?;
    assert_eq!(response.text()?, "info");

    // forwarded by a proxy, denied even with a local peer and a local forwarded ip
    let mut request = HttpRequest::new(Method::GET, server.url("/_sys/info"))?;
    request.headers.insert(HeaderName::from_static("x-forwarded-for"), HeaderValue::from_static("10.0.0.1"));
    let response = client.execute(request).await?;
    assert_eq!(response.status, 403);

    server.shutdown().await;
    Ok(())
}
