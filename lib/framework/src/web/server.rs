use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::MatchedPath;
use axum::extract::Request;
use axum::extract::State;
use axum::http::HeaderName;
use axum::http::StatusCode;
use axum::http::header;
use axum::http::uri::Authority;
use axum::http::uri::PathAndQuery;
use axum::middleware;
use axum::middleware::Next;
use axum::response::IntoResponse as _;
use axum::response::Response;
use axum_extra::extract::CookieJar;
use tokio::net::TcpListener;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
pub use tower_http::services::ServeDir;
pub use tower_http::services::ServeFile;

use crate::log;
use crate::log::LogValue;
use crate::metrics::Counter;
use crate::metrics::Metrics;
use crate::web::CLIENT;
use crate::web::REF_ID;
use crate::web::client_info::client_info;

const X_FORWARDED_PROTO: HeaderName = HeaderName::from_static("x-forwarded-proto");

pub struct HttpServerConfig {
    pub bind_address: String,
    pub max_forwarded_ips: usize,
    pub shutdown_grace_period: Duration,
}

impl Default for HttpServerConfig {
    fn default() -> Self {
        HttpServerConfig {
            bind_address: "0.0.0.0:8080".to_owned(),
            max_forwarded_ips: 2,
            shutdown_grace_period: Duration::ZERO,
        }
    }
}

pub struct HttpServer {
    config: HttpServerConfig,
    counter: Arc<Counter>,
}

#[derive(Clone)]
struct HttpServerState {
    counter: Arc<Counter>,
    max_forwarded_ips: usize,
}

impl HttpServer {
    pub fn new(config: HttpServerConfig) -> Self {
        Self { config, counter: Arc::default() }
    }

    pub fn metrics(&self) -> impl Fn(&mut Metrics) + Send + 'static {
        let counter = Arc::clone(&self.counter);
        move |metrics| {
            metrics.stats.push(("active_http_requests", counter.max() as u64));
        }
    }

    pub async fn start(self, router: Router, shutdown_signal: CancellationToken) {
        let state = HttpServerState { counter: self.counter, max_forwarded_ips: self.config.max_forwarded_ips };
        let app = Router::new();
        let app = app.merge(router);
        // layer after merge, so it runs after routing and MatchedPath is available
        let app = app.layer(middleware::from_fn_with_state(state, http_server_layer));
        let app = app.into_make_service_with_connect_info::<SocketAddr>();
        let listener = TcpListener::bind(&self.config.bind_address).await.expect("failed to bind address");
        console!("start http server, bind={}", self.config.bind_address);
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                shutdown_signal.cancelled().await;
                let period = self.config.shutdown_grace_period;
                if !period.is_zero() {
                    console!("http server shutdown in {period:?}");
                    sleep(period).await;
                }
            })
            .await
            .expect("failed to start http server");
        console!("http server stopped");
    }
}

async fn http_server_layer(State(state): State<HttpServerState>, mut request: Request, next: Next) -> Response {
    // skip log for health check
    if request.uri().path() == "/health-check" {
        return StatusCode::OK.into_response(); // gce lb health check requires to return 200
    }

    let HttpServerState { counter, max_forwarded_ips } = state;

    let ref_id = request.headers().get(REF_ID).and_then(|v| v.to_str().ok()).map(|id| vec![id.to_owned()]);

    let _counter = counter.increase();

    let response = log::action("http", ref_id, async {
        context!(uri = request_url(&request), method = request.method().as_str());

        for (name, value) in request.headers() {
            if name != header::COOKIE {
                log!("[header] {name}={:?}", LogValue::new(name.as_str(), value));
            }
        }
        let cookies = CookieJar::from_headers(request.headers());
        for cookie in cookies.iter() {
            log!("[cookie] {}={}", cookie.name(), LogValue::new(cookie.name(), cookie.value()));
        }

        let client_info = client_info(&request, max_forwarded_ips);
        context!(client_ip = &client_info.client_ip);
        if let Some(ref user_agent) = client_info.user_agent {
            context!(user_agent = user_agent);
        }
        request.extensions_mut().insert(Arc::new(client_info));

        if let Some(client) = request.headers().get(CLIENT) {
            context!(client = client.to_str()?);
        }

        let matched_path = request.extensions().get::<MatchedPath>().map(MatchedPath::as_str);
        if let Some(matched_path) = matched_path {
            context!(matched_path = matched_path);
        }

        if let Some(length) = request
            .headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| str::parse::<usize>(v).ok())
        {
            stats!(request_content_length = length);
        }

        let http_response = next.run(request).await;

        let status = http_response.status().as_u16();
        context!(response_status = status.to_string());
        for (name, value) in http_response.headers() {
            log!("[header] {name}={:?}", LogValue::new(name.as_str(), value));
        }
        Ok(http_response)
    })
    .await;
    if let Ok(response) = response { response } else { StatusCode::INTERNAL_SERVER_ERROR.into_response() }
}

// http/1.1 request uri is in origin-form (only path and query), rebuild the absolute url with scheme and host
fn request_url<T>(request: &http::Request<T>) -> String {
    let uri = request.uri();

    let scheme = request
        .headers()
        .get(X_FORWARDED_PROTO)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.split(',').next().unwrap_or(value).trim())
        .or_else(|| uri.scheme_str())
        .unwrap_or("http");

    let host = uri
        .authority()
        .map(Authority::as_str)
        .or_else(|| request.headers().get(header::HOST).and_then(|value| value.to_str().ok()));

    let path_and_query = uri.path_and_query().map_or("/", PathAndQuery::as_str);

    match host {
        Some(host) => {
            let mut url = String::with_capacity(scheme.len() + 3 + host.len() + path_and_query.len());
            url.push_str(scheme);
            url.push_str("://");
            url.push_str(host);
            url.push_str(path_and_query);
            url
        }
        None => path_and_query.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_url_with_host_header() {
        let request = Request::builder().uri("/503?key=value").header(header::HOST, "localhost:8080").body(()).unwrap();
        assert_eq!(request_url(&request), "http://localhost:8080/503?key=value");
    }

    #[test]
    fn request_url_with_forwarded_proto() {
        let request = Request::builder()
            .uri("/503")
            .header(header::HOST, "example.com")
            .header(X_FORWARDED_PROTO, "https, http")
            .body(())
            .unwrap();
        assert_eq!(request_url(&request), "https://example.com/503");
    }

    #[test]
    fn request_url_with_absolute_uri() {
        let request = Request::builder().uri("https://example.com/503").body(()).unwrap();
        assert_eq!(request_url(&request), "https://example.com/503");
    }

    #[test]
    fn request_url_without_host() {
        let request = Request::builder().uri("/503").body(()).unwrap();
        assert_eq!(request_url(&request), "/503");
    }
}
