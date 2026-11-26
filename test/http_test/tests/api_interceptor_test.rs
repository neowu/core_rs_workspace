use std::sync::Arc;

use framework::exception;
use framework::exception::Exception;
use framework::exception::Severity;
use framework::exception::error_code;
use framework::http::HeaderName;
use framework::http::HeaderValue;
use framework::http::HttpRequest;
use framework::http::Method;
use framework::json;
use framework::web::request::Request;
use framework::web::response::Response;
use framework::web::router::Next;
use framework_macro::api;
use framework_macro::integration_test;
use http_test::GreetResponse;
use http_test::TestServer;
use http_test::http_client;
use tokio::task_local;

#[api]
trait AccountService {
    #[get]
    #[path("/api/account")]
    async fn current(&self) -> Result<GreetResponse, Exception>;
}

struct User {
    name: String,
}

// service methods don't take `Request`, the interceptor passes data by scoping a task local around the rest of the chain,
// the value is dropped with the request future, nothing to clean up
task_local! {
    static USER: User;
}

async fn login(service: Arc<AccountServiceImpl>, request: Request, next: Next) -> Result<Response, Exception> {
    if request.header("token") != Some(service.token) {
        return Err(exception!("login required", severity = Severity::Warn, code = error_code::FORBIDDEN));
    }
    let user = User { name: "neo".to_owned() };
    USER.scope(user, next.run(request)).await
}

struct AccountServiceImpl {
    token: &'static str,
}

impl AccountService for AccountServiceImpl {
    async fn current(&self) -> Result<GreetResponse, Exception> {
        // panics outside the interceptor scope, e.g. in a spawned task, `try_with` returns an error instead
        let name = USER.with(|user| user.name.clone());
        Ok(GreetResponse { greeting: format!("hello, {name}") })
    }
}

#[integration_test]
async fn api_interceptor() -> Result<(), Exception> {
    // interceptors chained on `route` apply to all methods
    let service = Arc::new(AccountServiceImpl { token: "secret" });
    let mut server = TestServer::start(AccountService::route(service).intercept(login)).await;
    let http_client = http_client(false);

    let client = AccountServiceClient::new(Arc::clone(&http_client), server.url.clone());
    let error = client.current().await.unwrap_err();
    assert_eq!(error.code, Some(error_code::FORBIDDEN));
    assert!(error.message.contains("status=403"), "message={}", error.message);

    let mut request = HttpRequest::new(Method::GET, server.url("/api/account"))?;
    request.headers.insert(HeaderName::from_static("token"), HeaderValue::from_static("secret"));
    let response = http_client.execute(request).await?;
    assert_eq!(response.status, 200);
    let response: GreetResponse = json::from_json(response.text()?)?;
    assert_eq!(response.greeting, "hello, neo");

    server.shutdown().await;
    Ok(())
}
