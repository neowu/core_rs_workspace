use std::collections::HashMap;
use std::time::Duration;

pub use http::HeaderName;
pub use http::header;
use reqwest::Body;
use reqwest::Method;
use reqwest::Request;
use reqwest::Url;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;

use crate::exception::CoreRsResult;

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
    pub fn new(method: HttpMethod, url: String) -> Self {
        HttpRequest {
            method,
            url,
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
    pub async fn execute(&self, request: HttpRequest) -> CoreRsResult<HttpResponse> {
        let span = debug_span!("http_client", url = request.url, method = ?request.method);
        async {
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
                *http_request.body_mut() = Some(Body::from(body));
            }

            let response = self.client.execute(http_request).await?;
            let status = response.status().as_u16();
            let mut headers = HashMap::new();
            debug!(status, "[response]");
            for (key, value) in response.headers() {
                let value = value.to_str()?;
                debug!("[header] {key}={value}");
                headers.insert(key.to_owned(), value.to_string());
            }

            let body = response.text().await?;
            if let Some(content_type) = headers.get(&header::CONTENT_TYPE)
                && (content_type.contains("json") || content_type.contains("text"))
            {
                debug!("[response] body={body}");
            }

            Ok(HttpResponse { status, headers, body })
        }
        .instrument(span)
        .await
    }
}

impl Default for HttpClient {
    fn default() -> Self {
        HttpClient {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .pool_idle_timeout(Duration::from_secs(300))
                .connection_verbose(false)
                .build()
                .unwrap(),
        }
    }
}
