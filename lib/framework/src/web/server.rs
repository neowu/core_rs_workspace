use std::any::Any;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt as _;
use http::HeaderMap;
use http::HeaderName;
use http::Method;
use http::StatusCode;
use http::Uri;
use http::header;
use http::uri::Authority;
use http::uri::PathAndQuery;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::TokioExecutor;
use hyper_util::rt::TokioIo;
use hyper_util::rt::TokioTimer;
use hyper_util::server::conn::auto;
use hyper_util::server::graceful::GracefulShutdown;
use tokio::net::TcpListener;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use crate::exception::Exception;
use crate::log;
use crate::metrics::Counter;
use crate::metrics::Metrics;
use crate::web::CLIENT;
use crate::web::REF_ID;
use crate::web::request::Request;
use crate::web::request::cookies;
use crate::web::response::Body;
use crate::web::response::Response;
use crate::web::router::Matched;
use crate::web::router::Router;
use crate::web::router::Routes;

const X_FORWARDED_PROTO: HeaderName = HeaderName::from_static("x-forwarded-proto");

pub struct HttpServerConfig {
    pub bind_address: String,
    pub max_forwarded_ips: usize,
    /// Delay before stopping acceptance and starting graceful drain; not a drain timeout.
    pub shutdown_delay: Duration,
    pub max_body_size: usize,
}

impl Default for HttpServerConfig {
    fn default() -> Self {
        HttpServerConfig {
            bind_address: "0.0.0.0:8080".to_owned(),
            max_forwarded_ips: 2,
            shutdown_delay: Duration::ZERO,
            max_body_size: 2 * 1024 * 1024,
        }
    }
}

pub struct HttpServer {
    config: HttpServerConfig,
    counter: Arc<Counter>,
    sse_counter: Arc<Counter>,
}

struct Shared {
    routes: Routes,
    counter: Arc<Counter>,
    sse_counter: Arc<Counter>,
    // cancelled when acceptance stops, closes all sse streams
    sse_shutdown: CancellationToken,
    max_forwarded_ips: usize,
    max_body_size: usize,
}

impl HttpServer {
    pub fn new(config: HttpServerConfig) -> Self {
        Self { config, counter: Arc::default(), sse_counter: Arc::default() }
    }

    /// Peak active handlers (excluding health checks and response body streaming) and open sse streams
    /// between collections.
    pub fn metrics(&self) -> impl Fn(&mut Metrics) + Send + 'static {
        let counter = Arc::clone(&self.counter);
        let sse_counter = Arc::clone(&self.sse_counter);
        move |metrics| {
            metrics.add_stat("active_http_requests", counter.max() as u64);
            metrics.add_stat("active_sse_streams", sse_counter.max() as u64);
        }
    }

    pub async fn start(self, router: Router, shutdown_signal: CancellationToken) {
        let listener = TcpListener::bind(&self.config.bind_address).await.expect("failed to bind address");
        self.start_with_listener(listener, router, shutdown_signal).await;
    }

    /// Serves on an already bound listener, the configured bind address is ignored.
    pub async fn start_with_listener(self, listener: TcpListener, router: Router, shutdown_signal: CancellationToken) {
        let local_addr = listener.local_addr().expect("failed to get local address");
        let shared = Arc::new(Shared {
            routes: router.routes,
            counter: self.counter,
            sse_counter: self.sse_counter,
            sse_shutdown: CancellationToken::new(),
            max_forwarded_ips: self.config.max_forwarded_ips,
            max_body_size: self.config.max_body_size,
        });

        // serves http/1.1 and h2c (prior knowledge) on the same port, detected by h2 connection preface.
        // protocol detection has no timeout, expects to run behind an L7 load balancer that only forwards complete
        // requests, slow / idle clients are handled there
        let mut builder = auto::Builder::new(TokioExecutor::new());
        builder.http1().timer(TokioTimer::new()); // enables default 30s header read timeout
        builder.http2().timer(TokioTimer::new());

        console!("start http server, bind={local_addr}");
        let graceful = GracefulShutdown::new();
        let shutdown = async {
            shutdown_signal.cancelled().await;
            let delay = self.config.shutdown_delay;
            if !delay.is_zero() {
                console!("http server shutdown in {delay:?}");
                sleep(delay).await;
            }
        };
        tokio::pin!(shutdown);

        loop {
            let (stream, peer_addr) = tokio::select! {
                result = listener.accept() => match result {
                    Ok(accepted) => accepted,
                    Err(err) => {
                        // e.g. too many open files, back off instead of spinning
                        console!("WARN failed to accept connection, error={err}");
                        sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                },
                () = &mut shutdown => break,
            };
            let _nodelay = stream.set_nodelay(true);

            let shared = Arc::clone(&shared);
            let service =
                service_fn(move |request| handle(Arc::clone(&shared), peer_addr, request).map(Ok::<_, Infallible>));
            let io = TokioIo::new(stream);
            let connection = graceful.watch(builder.serve_connection(io, service).into_owned());
            tokio::spawn(async move {
                // client resets and timeouts are expected, not logged
                let _closed = connection.await;
            });
        }

        drop(listener);
        shared.sse_shutdown.cancel();
        graceful.shutdown().await;
        console!("http server stopped");
    }
}

// not an async fn, its params would be held twice (upvar and local), this future is spawned per h2 stream and moved
// by value several times, keep it small
#[allow(clippy::manual_async_fn)]
fn handle(
    shared: Arc<Shared>,
    peer_addr: SocketAddr,
    request: http::Request<Incoming>,
) -> impl Future<Output = http::Response<Body>> {
    async move {
        // skip log for health check, gce lb health check requires to return 200
        if request.uri().path() == "/health-check" {
            return Response::empty().status(StatusCode::OK).into_http();
        }

        let head = request.method() == Method::HEAD;
        let ref_ids = request.headers().get(REF_ID).and_then(|v| v.to_str().ok()).map(|id| vec![id.to_owned()]);
        let _counter = shared.counter.increase();

        // boxed for the same reason, one malloc instead of moving the whole action future
        let result = Box::pin(log::action("http", ref_ids, async {
            let (parts, body) = request.into_parts();
            Ok::<_, Exception>(
                process(&shared, Request::new(parts, body, peer_addr, shared.max_forwarded_ips, shared.max_body_size))
                    .await,
            )
        }))
        .await;
        let response = match result {
            Ok(response) => response,
            Err(err) => Response::error(&err),
        };
        let mut response = match response.status_code() {
            // hyper h2 still sends the body of 204 / 304 responses, which must not have content
            StatusCode::NO_CONTENT | StatusCode::NOT_MODIFIED => response.without_content(),
            _ if head => response.without_body(),
            _ => response,
        };
        // after the http action and the HEAD check, the sse handler runs as its own action
        if let Some(sse) = response.sse_body() {
            sse.start(&shared.sse_shutdown, &shared.sse_counter);
        }
        response.into_http()
    }
}

async fn process(shared: &Shared, request: Request) -> Response {
    log_request(&request);

    let response = match shared.routes.find(request.method(), request.path()) {
        Matched::Found(path, route) => {
            context!(path = path, fn = route.name);
            // invoke inside the async block, a handler may panic before returning its future
            let mut response = match AssertUnwindSafe(async { (route.handler)(request).await }).catch_unwind().await {
                Ok(Ok(response)) => response,
                Ok(Err(err)) => {
                    log!(exception = err);
                    Response::error(&err)
                }
                Err(panic) => {
                    let err = exception!(format!("handler panicked, error={}", panic_message(panic.as_ref())));
                    log!(exception = err);
                    Response::error(&err)
                }
            };
            if let Some(sse) = response.sse_body() {
                sse.route(path, route.name);
            }
            response
        }
        Matched::NotFound => Response::empty().status(StatusCode::NOT_FOUND),
        // no Allow header, these requests are mostly from vulnerability scans, don't hint available methods
        Matched::MethodNotAllowed => Response::empty().status(StatusCode::METHOD_NOT_ALLOWED),
    };

    context!(response_status = response.status_code().as_str());
    for (name, value) in response.headers() {
        log!("[header] {name}={value:?}");
    }
    response
}

fn log_request(request: &Request) {
    context!(uri = request_url(request.uri(), request.headers()), method = request.method().as_str());

    for (name, value) in request.headers() {
        if name != header::COOKIE {
            log!("[header] {name}={value:?}");
        } else if let Ok(cookie_header) = value.to_str() {
            for (cookie_name, cookie_value) in cookies(cookie_header) {
                log!("[cookie] {cookie_name}={cookie_value:?}");
            }
        }
    }

    context!(client_ip = request.client_ip());
    if let Some(user_agent) = request.user_agent() {
        context!(user_agent = user_agent);
    }
    if let Some(client) = request.header(CLIENT) {
        context!(client = client);
    }
    if let Some(length) = request.header(header::CONTENT_LENGTH).and_then(|v| v.parse::<u64>().ok()) {
        stats!(request_content_length = length);
    }
}

pub(crate) fn panic_message(panic: &(dyn Any + Send)) -> &str {
    panic
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("unknown")
}

fn header_str(headers: &HeaderMap, name: HeaderName) -> Option<&str> {
    headers.get(name)?.to_str().ok()
}

// http/1.1 request uri is in origin-form (only path and query), rebuild the absolute url with scheme and host
fn request_url(uri: &Uri, headers: &HeaderMap) -> String {
    let scheme = header_str(headers, X_FORWARDED_PROTO)
        .map(|value| value.split(',').next().unwrap_or(value).trim())
        .or_else(|| uri.scheme_str())
        .unwrap_or("http");

    let host = uri.authority().map(Authority::as_str).or_else(|| header_str(headers, header::HOST));

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
    use http::HeaderValue;

    use super::*;

    #[test]
    fn request_url_with_host_header() {
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, HeaderValue::from_static("localhost:8080"));
        let uri: Uri = "/503?key=value".parse().unwrap();
        assert_eq!(request_url(&uri, &headers), "http://localhost:8080/503?key=value");

        headers.insert(X_FORWARDED_PROTO, HeaderValue::from_static("https, http"));
        assert_eq!(request_url(&uri, &headers), "https://localhost:8080/503?key=value");
    }

    #[test]
    fn request_url_with_absolute_uri() {
        let uri: Uri = "http://example.com/path".parse().unwrap();
        assert_eq!(request_url(&uri, &HeaderMap::new()), "http://example.com/path");
    }

    #[test]
    fn request_url_without_host() {
        let uri: Uri = "/path".parse().unwrap();
        assert_eq!(request_url(&uri, &HeaderMap::new()), "/path");
    }
}
