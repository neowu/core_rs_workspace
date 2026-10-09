use std::sync::Arc;

use framework::exception::Exception;
use framework::web::request::Request;
use framework::web::response::Response;
use framework::web::router::Router;
use framework_macro::integration_test;
use futures::future::try_join_all;
use http_test::GreetRequest;
use http_test::GreetResponse;
use http_test::TestServer;
use reqwest::Version;

async fn greet(_state: Arc<()>, mut request: Request) -> Result<Response, Exception> {
    let request: GreetRequest = request.json().await?;
    Response::json(&GreetResponse { greeting: format!("hello, {}", request.name) })
}

async fn version(_state: Arc<()>, request: Request) -> Result<Response, Exception> {
    Ok(Response::text(format!("{:?}", request.version())))
}

#[integration_test]
async fn h2c() -> Result<(), Exception> {
    let router = Router::new().state(Arc::new(()), |r| r.post("/greet", greet).get("/version", version));
    let mut server = TestServer::start(router).await;

    let response = server.h2c.get(server.url("/version")).send().await?;
    assert_eq!(response.version(), Version::HTTP_2);
    assert_eq!(response.text().await?, "HTTP/2.0");

    let response = server.http1.get(server.url("/version")).send().await?;
    assert_eq!(response.version(), Version::HTTP_11);
    assert_eq!(response.text().await?, "HTTP/1.1");

    // concurrent streams multiplexed on one h2 connection
    let requests = (0..50).map(|i| {
        let request = server.h2c.post(server.url("/greet")).json(&GreetRequest { name: i.to_string() });
        async move {
            let response = request.send().await?;
            assert_eq!(response.version(), Version::HTTP_2);
            let body: GreetResponse = response.json().await?;
            assert_eq!(body.greeting, format!("hello, {i}"));
            Ok::<_, Exception>(())
        }
    });
    try_join_all(requests).await?;

    let response = server.h2c.get(server.url("/unknown")).send().await?;
    assert_eq!(response.status(), 404);

    server.shutdown().await;
    Ok(())
}
