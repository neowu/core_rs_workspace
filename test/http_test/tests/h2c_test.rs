use std::sync::Arc;

use framework::exception::Exception;
use framework::http::HttpRequest;
use framework::http::Method;
use framework::json;
use framework::web::request::Request;
use framework::web::response::Response;
use framework::web::router::Router;
use framework_macro::integration_test;
use futures::future::try_join_all;
use http_test::GreetRequest;
use http_test::GreetResponse;
use http_test::TestServer;
use http_test::execute;
use http_test::http_client;

async fn greet(_state: Arc<()>, mut request: Request) -> Result<Response, Exception> {
    let request: GreetRequest = request.json().await?;
    Response::json(&GreetResponse { greeting: format!("hello, {}", request.name) })
}

async fn version(_state: Arc<()>, request: Request) -> Result<Response, Exception> {
    Ok(Response::text(format!("{:?}", request.version())))
}

#[integration_test]
async fn h2c() -> Result<(), Exception> {
    let router =
        Router::new().state(Arc::new(())).route(Method::POST, "/greet", greet).route(Method::GET, "/version", version);
    let mut server = TestServer::start(router).await;

    let http1 = http_client(false);
    let h2c = http_client(true);

    let response = execute(&h2c, Method::GET, server.url("/version")).await?;
    assert_eq!(response.text()?, "HTTP/2.0");

    let response = execute(&http1, Method::GET, server.url("/version")).await?;
    assert_eq!(response.text()?, "HTTP/1.1");

    // concurrent streams multiplexed on one h2 connection
    let (server_ref, h2c_ref) = (&server, &h2c);
    let requests = (0..50).map(|i| async move {
        let mut request = HttpRequest::new(Method::POST, server_ref.url("/greet"))?;
        request.body(json::to_json(&GreetRequest { name: i.to_string() })?, "application/json");
        let response = h2c_ref.execute(request).await?;
        let body: GreetResponse = json::from_json(response.text()?)?;
        assert_eq!(body.greeting, format!("hello, {i}"));
        Ok::<_, Exception>(())
    });
    try_join_all(requests).await?;

    let response = execute(&h2c, Method::GET, server.url("/unknown")).await?;
    assert_eq!(response.status, 404);

    server.shutdown().await;
    Ok(())
}
