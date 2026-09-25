use std::net::TcpListener;
use std::time::Duration;

use axum::Router;
use framework::exception::Exception;
use framework::http::HttpClient;
use framework::http::HttpClientConfig;
use framework::http::HttpRequest;
use framework::http::Method;
use framework::system::CancellationToken;
use framework::web::server::HttpServer;
use framework::web::server::HttpServerConfig;
use serde::Deserialize;
use serde::Serialize;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio::time::timeout;

#[derive(Debug, Serialize, Deserialize)]
pub struct GreetRequest {
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GreetResponse {
    pub greeting: String,
}

pub struct TestServer {
    pub client: HttpClient,
    pub url: String,
    shutdown_signal: CancellationToken,
    task: JoinHandle<()>,
}

impl TestServer {
    pub async fn start(router: Router) -> Result<Self, Exception> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?;
        // HttpServer binds its own listener, so release the selected port before starting it.
        drop(listener);
        let http_server = HttpServer::new(HttpServerConfig { bind_address: address.to_string(), ..Default::default() });
        let shutdown_signal = CancellationToken::new();
        let task = tokio::spawn(http_server.start(router, shutdown_signal.clone()));
        let server = Self {
            client: HttpClient::new(HttpClientConfig { timeout: Duration::from_secs(2), ..Default::default() }),
            url: format!("http://{address}"),
            shutdown_signal,
            task,
        };

        timeout(Duration::from_secs(5), async {
            loop {
                assert!(!server.task.is_finished(), "http server exited before becoming ready");
                let request = server.request(Method::GET, "/health-check");
                if let Ok(response) = server.client.execute(request).await
                    && response.status == 200
                {
                    return;
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("http server did not start");

        Ok(server)
    }

    pub fn request(&self, method: Method, path: &str) -> HttpRequest {
        HttpRequest::new(method, format!("{}{path}", self.url))
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
