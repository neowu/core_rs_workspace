use std::time::Duration;

use framework::http::HttpClient;
use framework::http::HttpClientConfig;
use framework::http::HttpRequest;
use framework::http::Method;
use framework::system::CancellationToken;
use framework::web::router::Router;
use framework::web::server::HttpServer;
use framework::web::server::HttpServerConfig;
use framework_macro::Validate;
use reqwest::Client;
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

pub struct TestServer {
    pub url: String,
    /// framework client, for #[api] clients
    pub client: HttpClient,
    pub http1: Client,
    pub h2c: Client,
    shutdown_signal: CancellationToken,
    task: JoinHandle<()>,
}

impl TestServer {
    pub async fn start(router: Router) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("failed to bind");
        let url = format!("http://{}", listener.local_addr().expect("failed to get local address"));
        let shutdown_signal = CancellationToken::new();
        let http_server = HttpServer::new(HttpServerConfig { max_body_size: 1024, ..Default::default() });
        let task = tokio::spawn(http_server.start_with_listener(listener, router, shutdown_signal.clone()));
        let server = Self {
            url,
            client: HttpClient::new(HttpClientConfig { timeout: Duration::from_secs(2), ..Default::default() }),
            http1: Client::builder()
                .http1_only()
                .timeout(Duration::from_secs(2))
                .build()
                .expect("failed to build client"),
            h2c: Client::builder()
                .http2_prior_knowledge()
                .timeout(Duration::from_secs(2))
                .build()
                .expect("failed to build client"),
            shutdown_signal,
            task,
        };

        timeout(Duration::from_secs(5), async {
            loop {
                assert!(!server.task.is_finished(), "http server exited before becoming ready");
                if let Ok(response) = server.http1.get(server.url("/health-check")).send().await
                    && response.status() == 200
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

    pub fn request(&self, method: Method, path: &str) -> HttpRequest {
        HttpRequest::new(method, self.url(path))
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
