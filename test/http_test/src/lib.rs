use std::sync::Arc;
use std::time::Duration;

use framework::exception::Exception;
use framework::http::HttpClient;
use framework::http::HttpClientConfig;
use framework::http::HttpRequest;
use framework::http::HttpResponse;
use framework::http::Method;
use framework::system::CancellationToken;
use framework::web::router::Router;
use framework::web::server::HttpServer;
use framework::web::server::HttpServerConfig;
use framework_macro::Validate;
use serde::Deserialize;
use serde::Serialize;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio::time::timeout;

#[derive(Debug, Serialize, Deserialize, Validate)]
pub struct GreetRequest {
    #[not_blank]
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GreetResponse {
    pub greeting: String,
}

// a new client per call, h2c = http2 prior knowledge, otherwise http1; Arc for #[api] clients
pub fn http_client(h2c: bool) -> Arc<HttpClient> {
    Arc::new(HttpClient::new(HttpClientConfig {
        timeout: Duration::from_secs(2),
        prefer_http2: h2c,
        ..Default::default()
    }))
}

pub async fn execute(client: &HttpClient, method: Method, url: String) -> Result<HttpResponse, Exception> {
    client.execute(HttpRequest::new(method, url)?).await
}

pub struct TestServer {
    pub url: String,
    shutdown_signal: CancellationToken,
    task: JoinHandle<()>,
}

impl TestServer {
    pub async fn start(router: impl Into<Router>) -> TestServer {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("failed to bind");
        let url = format!("http://{}", listener.local_addr().expect("failed to get local address"));
        let shutdown_signal = CancellationToken::new();
        let http_server = HttpServer::new(HttpServerConfig { max_body_size: 1024, ..Default::default() });
        let task = tokio::spawn(http_server.start_with_listener(listener, router.into(), shutdown_signal.clone()));
        let server = TestServer { url, shutdown_signal, task };

        timeout(Duration::from_secs(5), async {
            let client = http_client(false);
            loop {
                assert!(!server.task.is_finished(), "http server exited before becoming ready");
                if let Ok(response) = execute(&client, Method::GET, server.url("/health-check")).await
                    && response.status == 200
                {
                    return;
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("http server did not start");

        server
    }

    pub fn url(&self, path: &str) -> String {
        format!("{}{path}", self.url)
    }

    pub async fn shutdown(&mut self) {
        self.shutdown_signal.cancel();
        timeout(Duration::from_secs(5), &mut self.task)
            .await
            .expect("http server did not stop")
            .expect("http server task failed");
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.shutdown_signal.cancel();
        self.task.abort();
    }
}
