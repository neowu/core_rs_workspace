use std::collections::HashMap;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use futures::Stream;
use futures::TryStreamExt;
pub use http::HeaderName;
pub use http::header;
use reqwest::Body;
use reqwest::Method;
use reqwest::Request;
use reqwest::Url;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;

use crate::exception::Exception;

#[derive(Clone)]
pub struct HttpClient {
    client: reqwest::Client,
}

pub struct HttpRequest {
    pub method: HttpMethod,
    pub url: String,
    pub headers: HashMap<HeaderName, String>,
    body: Option<String>,
}

impl HttpRequest {
    pub fn new(method: HttpMethod, url: impl Into<String>) -> Self {
        HttpRequest {
            method,
            url: url.into(),
            headers: HashMap::new(),
            body: None,
        }
    }

    pub fn body(&mut self, body: String, content_type: impl Into<String>) {
        self.body = Some(body);
        self.headers.insert(header::CONTENT_TYPE, content_type.into());
    }
}

#[derive(Debug)]
pub enum HttpMethod {
    GET,
    POST,
    PUT,
    DELETE,
}

impl From<HttpMethod> for Method {
    fn from(method: HttpMethod) -> Self {
        match method {
            HttpMethod::GET => Method::GET,
            HttpMethod::POST => Method::POST,
            HttpMethod::PUT => Method::PUT,
            HttpMethod::DELETE => Method::DELETE,
        }
    }
}

pub struct HttpResponse {
    pub status: u16,
    pub headers: HashMap<HeaderName, String>,
    pub body: String,
}

impl HttpClient {
    pub async fn execute(&self, request: HttpRequest) -> Result<HttpResponse, Exception> {
        let span = debug_span!("http", url = request.url, method = ?request.method);
        async {
            let http_request = create_request(request)?;

            let response = self.client.execute(http_request).await?;
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
        let span = debug_span!("http", url = request.url, method = ?request.method);
        async {
            request.headers.insert(header::ACCEPT, "text/event-stream".to_string());
            let http_request = create_request(request)?;

            let response = self.client.execute(http_request).await?;
            let status = response.status().as_u16();
            debug!(status, "[response]");

            let headers = parse_headers(&response)?;

            if status == 200
                && let Some(content_type) = headers.get(&header::CONTENT_TYPE)
                && content_type.starts_with("text/event-stream")
            {
                let stream = response.bytes_stream();
                Ok(EventSource::new(Box::pin(stream.map_err(|e| e.into()))))
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
        headers.insert(key.to_owned(), value.to_string());
    }
    Ok(headers)
}

fn create_request(request: HttpRequest) -> Result<Request, Exception> {
    debug!(method = ?request.method, "[request]");
    debug!(url = request.url, "[request]");
    let url = Url::parse(&request.url)?;
    let mut http_request = Request::new(request.method.into(), url);
    for (key, value) in request.headers {
        debug!("[header] {}={}", key, value);
        http_request.headers_mut().insert(key, value.parse()?);
    }
    if let Some(body) = request.body {
        debug!("[request] body={body}");
        debug!(http_write_bytes = body.len(), "stats");
        *http_request.body_mut() = Some(Body::from(body));
    }
    Ok(http_request)
}

impl Default for HttpClient {
    fn default() -> Self {
        HttpClient {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .danger_accept_invalid_certs(true)
                .pool_idle_timeout(Duration::from_secs(300))
                .connection_verbose(false)
                .build()
                .unwrap(),
        }
    }
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
                            let current_bytes = std::mem::take(&mut self.buffer);
                            let line = String::from_utf8_lossy(&current_bytes);
                            debug!("[sse] {line}");
                            self.read_bytes += line.len();

                            if line.is_empty() {
                                continue;
                            } else if let Some(index) = line.find(": ") {
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
            };
        }
    }
}
