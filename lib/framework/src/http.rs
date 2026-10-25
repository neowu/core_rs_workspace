use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use futures::TryStreamExt as _;
pub use http::HeaderMap;
pub use http::HeaderName;
use http::HeaderValue;
pub use http::Method;
use http::Request;
use http::Response;
use http::Uri;
pub use http::header;
use http::uri::Authority;
use http::uri::PathAndQuery;
use http_body_util::BodyDataStream;
use http_body_util::BodyExt as _;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::client::conn::http1;
use hyper::client::conn::http2;
use hyper::rt::Read;
use hyper::rt::Write;
use hyper_util::rt::TokioExecutor;
use hyper_util::rt::TokioIo;
use socket2::SockRef;
use socket2::TcpKeepalive;
use tokio::net::TcpStream;
use tokio::time;
use tokio::time::sleep;
use tokio::time::timeout;
use tokio::time::timeout_at;
use tokio_rustls::TlsConnector;
pub use tokio_rustls::rustls::pki_types::CertificateDer;
use tokio_rustls::rustls::pki_types::ServerName;
pub use tokio_stream::StreamExt;

use crate::exception::Exception;
use crate::log;
use crate::span;
use crate::stats;
use crate::warn;

mod dns;
pub use dns::FallbackDnsResolver;
mod sse;
pub use sse::EventSource;
mod tls;

const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(90);
// bound sockets kept after a burst of concurrent http1 requests
const POOL_MAX_IDLE_PER_ORIGIN: usize = 32;
// detect peers silently dropped by lb/nat, same as reqwest default
const TCP_KEEPALIVE: Duration = Duration::from_secs(15);

pub struct HttpClient {
    tls: TlsConnector,
    resolver: Option<FallbackDnsResolver>,
    prefer_http2: bool,
    timeout: Duration,
    retry: RetryConfig,
    // key = scheme://host:port
    pool: Mutex<HashMap<String, Vec<IdleConnection>>>,
}

#[derive(Clone)]
pub struct RetryConfig {
    pub max_attempts: u32,
    pub interval: Duration,
}

pub struct HttpClientConfig {
    pub accept_invalid_cert: bool,
    // only trust these certs instead of platform roots
    pub accept_certs: Option<Vec<CertificateDer<'static>>>,
    pub timeout: Duration,
    pub retry: RetryConfig,
    // h2c for http, https always negotiates via alpn
    pub prefer_http2: bool,
    // reuse the last resolved addrs when dns fails, for stable service ips, refer to FallbackDnsResolver
    pub enable_fallback_dns_cache: bool,
}

impl Default for HttpClientConfig {
    // for docker image used on cloud env, must install "ca-certificates"
    fn default() -> HttpClientConfig {
        HttpClientConfig {
            accept_invalid_cert: false,
            accept_certs: None,
            timeout: Duration::from_secs(30),
            retry: RetryConfig { max_attempts: 1, interval: Duration::from_millis(500) },
            prefer_http2: false,
            enable_fallback_dns_cache: false,
        }
    }
}

impl HttpClientConfig {
    // use to call internal services with self signed certs and http2
    pub const fn internal_only() -> HttpClientConfig {
        HttpClientConfig {
            accept_invalid_cert: true,
            accept_certs: Some(vec![]),
            timeout: Duration::from_secs(30),
            retry: RetryConfig { max_attempts: 3, interval: Duration::from_millis(500) },
            prefer_http2: true,
            enable_fallback_dns_cache: false,
        }
    }
}

pub struct HttpRequest {
    pub method: Method,
    uri: Uri,
    https: bool,
    host: String,
    port: u16,
    // pool key = scheme://host:port
    key: String,
    headers: HeaderMap,
    // Bytes to share across retries without copying
    body: Option<Bytes>,
}

impl HttpRequest {
    pub fn new(method: Method, url: impl Into<String>) -> Result<HttpRequest, Exception> {
        let url = url.into();
        let uri: Uri = url.parse().map_err(|err| exception!(format!("invalid url, url={url}"), source = err))?;
        let https = match uri.scheme_str() {
            Some("https") => true,
            Some("http") => false,
            _ => return Err(exception!(format!("invalid url scheme, url={url}"))),
        };
        // ipv6 host is in brackets, e.g. [::1]
        let host = uri
            .host()
            .ok_or_else(|| exception!(format!("invalid url host, url={url}")))?
            .trim_start_matches('[')
            .trim_end_matches(']')
            .to_owned();
        // uri parsing doesn't validate port, port_u16() returns None for both absent and invalid port
        let authority = uri.authority().map_or("", Authority::as_str);
        let host_port = authority.rsplit_once('@').map_or(authority, |(_, host_port)| host_port);
        let port = match host_port.rsplit_once(':') {
            Some((_, port)) if !port.is_empty() && !port.ends_with(']') => {
                port.parse().map_err(|err| exception!(format!("invalid url port, url={url}"), source = err))?
            }
            _ => {
                if https {
                    443
                } else {
                    80
                }
            }
        };
        let key = format!("{}://{host}:{port}", if https { "https" } else { "http" });
        Ok(HttpRequest { method, uri, https, host, port, key, headers: HeaderMap::new(), body: None })
    }

    pub fn header(&mut self, name: HeaderName, value: &str) -> Result<(), Exception> {
        self.headers.insert(
            name,
            HeaderValue::from_str(value)
                .map_err(|err| exception!(format!("invalid header value, value={value}"), source = err))?,
        );
        Ok(())
    }

    pub fn body(&mut self, body: String, content_type: &'static str) {
        self.body = Some(Bytes::from(body));
        self.headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    }
}

pub struct HttpResponse {
    pub status: u16,
    pub headers: HeaderMap,
    pub body: String,
}

enum Connection {
    Http1(http1::SendRequest<Full<Bytes>>),
    Http2(http2::SendRequest<Full<Bytes>>),
}

impl Connection {
    async fn send(&mut self, request: Request<Full<Bytes>>) -> Result<Response<Incoming>, hyper::Error> {
        match self {
            Connection::Http1(sender) => sender.send_request(request).await,
            Connection::Http2(sender) => sender.send_request(request).await,
        }
    }

    async fn ready(&mut self) -> Result<(), hyper::Error> {
        match self {
            Connection::Http1(sender) => sender.ready().await,
            Connection::Http2(sender) => sender.ready().await,
        }
    }

    fn is_closed(&self) -> bool {
        match self {
            Connection::Http1(sender) => sender.is_closed(),
            Connection::Http2(sender) => sender.is_closed(),
        }
    }

    // http2 multiplexes requests on one connection, can be shared via pool while in use
    fn shared(&self) -> Option<Connection> {
        match self {
            Connection::Http1(_) => None,
            Connection::Http2(sender) => Some(Connection::Http2(sender.clone())),
        }
    }
}

struct IdleConnection {
    connection: Connection,
    idle_at: Instant,
}

impl HttpClient {
    pub fn new(config: HttpClientConfig) -> HttpClient {
        HttpClient {
            tls: TlsConnector::from(Arc::new(tls::tls_config(&config))),
            resolver: config.enable_fallback_dns_cache.then(FallbackDnsResolver::default),
            prefer_http2: config.prefer_http2,
            timeout: config.timeout,
            retry: config.retry,
            pool: Mutex::new(HashMap::new()),
        }
    }

    pub async fn execute(&self, request: HttpRequest) -> Result<HttpResponse, Exception> {
        let _span = span!("http");
        let max_attempts = self.retry.max_attempts.max(1);
        let interval = self.retry.interval;

        let mut attempt: u32 = 0;
        let (deadline, connection, response) = loop {
            attempt += 1;
            // timeout covers connect, request and reading body
            let deadline = time::Instant::now() + self.timeout;
            let result = timeout_at(deadline, self.send(&request))
                .await
                .map_err(|err| exception!("http request timeout", source = err))
                .flatten();
            match result {
                Ok((connection, response)) => {
                    let status = response.status().as_u16();
                    if status == 503 && attempt < max_attempts {
                        warn!(
                            error_code = "HTTP_REQUEST_FAILED",
                            "http request failed, retry soon, attempt={attempt}, status={status}"
                        );
                        sleep(interval * attempt).await;
                        continue;
                    }
                    break (deadline, connection, response);
                }
                Err(err) => {
                    if request.method.is_idempotent() && attempt < max_attempts {
                        warn!(
                            error_code = "HTTP_REQUEST_FAILED",
                            "http request failed, retry soon, attempt={attempt}, error={err}"
                        );
                        sleep(interval * attempt).await;
                        continue;
                    }
                    return Err(exception!("http request failed", code = "HTTP_REQUEST_FAILED", source = err));
                }
            }
        };

        let status = response.status().as_u16();
        log!("[response] status={status}");

        let (parts, body) = response.into_parts();
        let headers = parts.headers;
        log_headers(&headers);

        let body = timeout_at(deadline, body.collect())
            .await
            .map_err(|err| exception!("http request timeout", source = err))??
            .to_bytes();
        // http1 connection is reusable only after response body is fully read
        if matches!(connection, Connection::Http1(_)) {
            self.release(&request.key, connection);
        }
        let body = String::from_utf8(Vec::from(body))
            .unwrap_or_else(|err| String::from_utf8_lossy(err.as_bytes()).into_owned());
        if let Some(content_type) = headers.get(header::CONTENT_TYPE).and_then(|value| value.to_str().ok())
            && (content_type.contains("json") || content_type.contains("text"))
        {
            log!("[response] body={body}");
        }
        stats!(http_read_bytes = body.len());

        Ok(HttpResponse { status, headers, body })
    }

    pub async fn sse(&self, mut request: HttpRequest) -> Result<EventSource, Exception> {
        let _span = span!("http");
        request.headers.insert(header::ACCEPT, HeaderValue::from_static("text/event-stream"));

        // timeout only covers until response headers, the event stream is long lived
        // http1 connection is not returned to pool, it closes after stream ends
        let (_, response) = timeout(self.timeout, self.send(&request))
            .await
            .map_err(|err| exception!("http request timeout", source = err))
            .flatten()?;
        let status = response.status().as_u16();
        log!("[response] status={status}");

        log_headers(response.headers());

        let content_type = response.headers().get(header::CONTENT_TYPE).cloned();
        if status == 200
            && let Some(ref content_type) = content_type
            && content_type.as_bytes().starts_with(b"text/event-stream")
        {
            let stream = BodyDataStream::new(response.into_body());
            Ok(EventSource::new(Box::pin(stream.map_err(Exception::from))))
        } else {
            let body = timeout(self.timeout, response.into_body().collect())
                .await
                .map_err(|err| exception!("http request timeout", source = err))??
                .to_bytes();
            log!("[response] body={}", String::from_utf8_lossy(&body));
            Err(exception!(format!("invalid sse response, status={status}, content_type={content_type:?}")))
        }
    }

    async fn send(&self, request: &HttpRequest) -> Result<(Connection, Response<Incoming>), Exception> {
        loop {
            let (mut connection, pooled) = match self.checkout(&request.key).await {
                Some(connection) => (connection, true),
                None => (self.connect(request).await?, false),
            };
            if let Some(shared) = connection.shared() {
                self.release(&request.key, shared);
            }
            let http_request = create_request(request, matches!(connection, Connection::Http2(_)))?;
            match connection.send(http_request).await {
                Ok(response) => return Ok((connection, response)),
                // pooled connection was closed by peer before request was sent, safe to retry
                Err(err) if pooled && err.is_canceled() => {}
                Err(err) => return Err(err.into()),
            }
        }
    }

    async fn checkout(&self, key: &str) -> Option<Connection> {
        loop {
            let IdleConnection { mut connection, idle_at } = self.pool.lock().unwrap().get_mut(key)?.pop()?;
            if idle_at.elapsed() < POOL_IDLE_TIMEOUT && connection.ready().await.is_ok() {
                return Some(connection);
            }
        }
    }

    fn release(&self, key: &str, connection: Connection) {
        let mut pool = self.pool.lock().unwrap();
        let connections = pool.entry(key.to_owned()).or_default();
        connections.retain(|idle| !idle.connection.is_closed() && idle.idle_at.elapsed() < POOL_IDLE_TIMEOUT);
        if connections.len() >= POOL_MAX_IDLE_PER_ORIGIN {
            // evict the oldest, least likely to be reused as checkout pops the newest
            connections.remove(0);
        }
        connections.push(IdleConnection { connection, idle_at: Instant::now() });
    }

    async fn connect(&self, request: &HttpRequest) -> Result<Connection, Exception> {
        log!("[connect] {}", request.key);
        let stream = if let Some(resolver) = &self.resolver {
            let mut addrs = resolver.lookup(&request.host).await?;
            for addr in &mut addrs {
                addr.set_port(request.port);
            }
            TcpStream::connect(addrs.as_slice()).await?
        } else {
            TcpStream::connect((request.host.as_str(), request.port)).await?
        };
        stream.set_nodelay(true)?;
        SockRef::from(&stream).set_tcp_keepalive(
            &TcpKeepalive::new().with_time(TCP_KEEPALIVE).with_interval(TCP_KEEPALIVE).with_retries(3),
        )?;

        if request.https {
            let server_name = ServerName::try_from(request.host.clone())?;
            let stream = self.tls.connect(server_name, stream).await?;
            let http2 = stream.get_ref().1.alpn_protocol() == Some(b"h2");
            handshake(TokioIo::new(stream), http2).await
        } else {
            handshake(TokioIo::new(stream), self.prefer_http2).await
        }
    }
}

async fn handshake<T>(io: T, http2: bool) -> Result<Connection, Exception>
where
    T: Read + Write + Unpin + Send + 'static,
{
    if http2 {
        let (sender, connection) = http2::handshake(TokioExecutor::new(), io).await?;
        tokio::spawn(connection);
        Ok(Connection::Http2(sender))
    } else {
        let (sender, connection) = http1::handshake(io).await?;
        tokio::spawn(connection);
        Ok(Connection::Http1(sender))
    }
}

fn log_headers(headers: &HeaderMap) {
    for (key, value) in headers {
        log!("[header] {key}={value:?}");
    }
}

fn create_request(request: &HttpRequest, http2: bool) -> Result<Request<Full<Bytes>>, Exception> {
    log!("[request] method={}, url={}", request.method.as_str(), request.uri);
    let mut builder = Request::builder().method(request.method.clone());
    // http2 takes :scheme/:authority from absolute uri, http1 sends origin-form uri with host header
    builder = if http2 {
        builder.uri(request.uri.clone())
    } else {
        builder
            .uri(request.uri.path_and_query().map_or("/", PathAndQuery::as_str))
            .header(header::HOST, request.uri.authority().map_or("", Authority::as_str))
    };
    let mut http_request = builder.body(Full::new(request.body.clone().unwrap_or_default()))?;
    http_request.headers_mut().extend(request.headers.clone());
    log_headers(http_request.headers());
    if let Some(ref body) = request.body {
        log!("[request] body={}", String::from_utf8_lossy(body));
        stats!(http_write_bytes = body.len());
    }
    Ok(http_request)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_url() {
        let port = |url| HttpRequest::new(Method::GET, url).map(|request| request.port).ok();
        assert_eq!(port("http://example.com/path"), Some(80));
        assert_eq!(port("https://example.com"), Some(443));
        assert_eq!(port("http://example.com:8080/path"), Some(8080));
        assert_eq!(port("http://user:pass@example.com/path"), Some(80));
        assert_eq!(port("http://[::1]/path"), Some(80));
        assert_eq!(port("http://[::1]:8080/path"), Some(8080));
        assert_eq!(port("http://example.com:99999/path"), None);
        assert_eq!(port("https://example.com:abc"), None);
        assert_eq!(port("http://[::1]:99999"), None);
    }
}
