use std::collections::HashMap;
use std::collections::VecDeque;
use std::mem;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use futures::Stream;
use futures::TryStreamExt as _;
pub use http::HeaderName;
pub use http::header;
use reqwest::Body;
use reqwest::Certificate;
use reqwest::Method;
use reqwest::Request;
use reqwest::Url;
use tokio::time::sleep;
use tracing::Instrument as _;
use tracing::debug;
use tracing::debug_span;
use tracing::warn;

use crate::exception::Exception;
use crate::exception::Severity;

#[derive(Clone)]
pub struct HttpClient {
    client: reqwest::Client,
    retry: RetryConfig,
}

#[allow(unused)]
#[derive(Clone)]
pub struct RetryConfig {
    pub max_attempts: u32,
    pub interval: Duration,
}

#[allow(unused)]
pub struct HttpClientConfig {
    pub accept_invalid_cert: bool,
    pub accept_certs: Option<Vec<Certificate>>,
    pub timeout: Duration,
    pub retry: RetryConfig,
}

impl Default for HttpClientConfig {
    // for docker image used on cloud env, must install "ca-certificates"
    fn default() -> Self {
        Self {
            accept_invalid_cert: false,
            accept_certs: None,
            timeout: Duration::from_secs(30),
            retry: RetryConfig { max_attempts: 1, interval: Duration::from_millis(500) },
        }
    }
}

impl HttpClientConfig {
    // use to call internal services with self signed certs
    pub fn internal_only() -> Self {
        Self {
            accept_invalid_cert: true,
            accept_certs: Some(vec![]),
            timeout: Duration::from_secs(30),
            retry: RetryConfig { max_attempts: 3, interval: Duration::from_millis(500) },
        }
    }
}

pub struct HttpRequest {
    pub method: HttpMethod,
    pub url: String,
    pub headers: HashMap<HeaderName, String>,
    body: Option<String>,
}

impl HttpRequest {
    pub fn new(method: HttpMethod, url: impl Into<String>) -> Self {
        HttpRequest { method, url: url.into(), headers: HashMap::new(), body: None }
    }

    pub fn body(&mut self, body: String, content_type: impl Into<String>) {
        self.body = Some(body);
        self.headers.insert(header::CONTENT_TYPE, content_type.into());
    }
}

#[derive(Debug, Clone)]
pub enum HttpMethod {
    Get,
    Post,
    Put,
    Delete,
}

impl HttpMethod {
    fn to_str(&self) -> &'static str {
        match self {
            HttpMethod::Get => "GET",
            HttpMethod::Post => "POST",
            HttpMethod::Put => "PUT",
            HttpMethod::Delete => "DELETE",
        }
    }
}

impl From<HttpMethod> for Method {
    fn from(method: HttpMethod) -> Self {
        match method {
            HttpMethod::Get => Method::GET,
            HttpMethod::Post => Method::POST,
            HttpMethod::Put => Method::PUT,
            HttpMethod::Delete => Method::DELETE,
        }
    }
}

pub struct HttpResponse {
    pub status: u16,
    pub headers: HashMap<HeaderName, String>,
    pub body: String,
}

impl HttpClient {
    pub fn new(config: HttpClientConfig) -> Self {
        let mut builder = reqwest::Client::builder()
            .timeout(config.timeout)
            .tls_danger_accept_invalid_certs(config.accept_invalid_cert)
            .pool_idle_timeout(Duration::from_mins(5))
            .http2_prior_knowledge()
            .connection_verbose(false);

        if let Some(certs) = config.accept_certs {
            builder = builder.tls_certs_only(certs);
        }

        let client = builder.build().expect("build cannot fail");
        HttpClient { client, retry: config.retry }
    }

    pub async fn execute(&self, request: HttpRequest) -> Result<HttpResponse, Exception> {
        let span = debug_span!("http");
        async {
            let max_attempts = self.retry.max_attempts.max(1);
            let interval = self.retry.interval;
            let idempotent = matches!(request.method, HttpMethod::Get | HttpMethod::Put | HttpMethod::Delete);

            let mut attempt: u32 = 0;
            let response = loop {
                attempt += 1;
                let http_request = create_request(&request)?;
                match self.client.execute(http_request).await {
                    Ok(response) => {
                        let status = response.status().as_u16();
                        if status == 503 && attempt < max_attempts {
                            warn!(
                                error_code = "HTTP_REQUEST_FAILED",
                                attempt, status, "http request failed, retry soon"
                            );
                            sleep(interval * attempt).await;
                            continue;
                        }
                        break response;
                    }
                    Err(err) => {
                        if idempotent && attempt < max_attempts {
                            warn!(
                                "{:?}",
                                exception!(
                                    severity = Severity::Warn,
                                    code = "HTTP_REQUEST_FAILED",
                                    message = "http request failed, retry soon",
                                    source = err
                                )
                            );
                            sleep(interval * attempt).await;
                            continue;
                        }
                        return Err(exception!(
                            code = "HTTP_REQUEST_FAILED",
                            message = "http request failed",
                            source = err
                        ));
                    }
                }
            };

            let status = response.status().as_u16();
            debug!(status, "[response]");

            let headers = parse_headers(&response)?;

            let body = response.text().await?;
            if let Some(content_type) = headers.get(&header::CONTENT_TYPE)
                && (content_type.contains("json") || content_type.contains("text"))
            {
                debug!("[response] body={body}");
            }
            debug!(http_read_bytes = body.len(), "stats");

            Ok(HttpResponse { status, headers, body })
        }
        .instrument(span)
        .await
    }

    pub async fn sse(&self, mut request: HttpRequest) -> Result<EventSource, Exception> {
        let span = debug_span!("sse");
        async {
            request.headers.insert(header::ACCEPT, "text/event-stream".to_owned());
            let http_request = create_request(&request)?;

            let response = self.client.execute(http_request).await?;
            let status = response.status().as_u16();
            debug!(status, "[response]");

            let headers = parse_headers(&response)?;

            if status == 200
                && let Some(content_type) = headers.get(&header::CONTENT_TYPE)
                && content_type.starts_with("text/event-stream")
            {
                let stream = response.bytes_stream();
                Ok(EventSource::new(Box::pin(stream.map_err(reqwest::Error::into))))
            } else {
                let body = response.text().await?;
                debug!("[response] body={body}");
                let content_type = headers.get(&header::CONTENT_TYPE);
                Err(exception!(
                    message = format!("invalid sse response, status={status}, content_type={content_type:?}")
                ))
            }
        }
        .instrument(span)
        .await
    }
}

fn parse_headers(response: &reqwest::Response) -> Result<HashMap<HeaderName, String>, Exception> {
    let mut headers = HashMap::new();
    for (key, value) in response.headers() {
        let value = value.to_str()?;
        debug!("[header] {key}={value}");
        headers.insert(key.to_owned(), value.to_owned());
    }
    Ok(headers)
}

fn create_request(request: &HttpRequest) -> Result<Request, Exception> {
    debug!(method = request.method.to_str(), "[request]");
    debug!(url = request.url, "[request]");
    let url = Url::parse(&request.url)?;
    let mut http_request = Request::new(request.method.clone().into(), url);
    for (key, value) in &request.headers {
        debug!("[header] {}={}", key, value);
        http_request.headers_mut().insert(key, value.parse()?);
    }
    if let Some(ref body) = request.body {
        debug!("[request] body={body}");
        debug!(http_write_bytes = body.len(), "stats");
        *http_request.body_mut() = Some(Body::from(body.to_owned()));
    }
    Ok(http_request)
}

#[derive(Debug)]
pub struct Event {
    pub id: Option<String>,
    pub r#type: Option<String>,
    pub data: String,
}

pub struct EventSource {
    response: Pin<Box<dyn Stream<Item = Result<Bytes, Exception>> + Send>>,
    buffer: Vec<u8>,
    events: VecDeque<Event>,
    last_id: Option<String>,
    last_type: Option<String>,

    start_time: Instant,
    read_bytes: usize,
    read_entries: usize,
}

impl EventSource {
    fn new(response: Pin<Box<dyn Stream<Item = Result<Bytes, Exception>> + Send>>) -> Self {
        EventSource {
            response,
            buffer: vec![],
            events: VecDeque::new(),
            last_id: None,
            last_type: None,
            start_time: Instant::now(),
            read_bytes: 0,
            read_entries: 0,
        }
    }
}

impl Drop for EventSource {
    fn drop(&mut self) {
        debug!(
            sse_read_entries = self.read_entries,
            sse_read_bytes = self.read_bytes,
            sse_elapsed = self.start_time.elapsed().as_nanos(),
            sse_count = 1,
            "stats"
        );
    }
}

impl Stream for EventSource {
    type Item = Result<Event, Exception>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(event) = self.events.pop_front() {
                return Poll::Ready(Some(Ok(event)));
            }

            match self.response.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(bytes))) => {
                    for byte in bytes {
                        if byte == b'\n' {
                            let current_bytes = mem::take(&mut self.buffer);
                            let line = String::from_utf8_lossy(&current_bytes);
                            debug!("[sse] {line}");
                            self.read_bytes += line.len();

                            if !line.is_empty()
                                && let Some(index) = line.find(": ")
                            {
                                let field = &line[0..index];
                                match field {
                                    "id" => self.last_id = Some(line[index + 2..].to_string()),
                                    "event" => self.last_type = Some(line[index + 2..].to_string()),
                                    "data" => {
                                        let id = self.last_id.take();
                                        let r#type = self.last_type.take();
                                        self.events.push_back(Event {
                                            id,
                                            r#type,
                                            data: line[index + 2..].to_string(),
                                        });
                                        self.read_entries += 1;
                                    }
                                    _ => {}
                                }
                            }
                        } else {
                            self.buffer.push(byte);
                        }
                    }
                }
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Some(Err(err))),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}
