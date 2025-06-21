use std::collections::HashMap;
use std::io;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use futures::AsyncBufReadExt;
use futures::Stream;
use futures::TryStreamExt;
use futures::io::Lines;
use futures::stream::IntoAsyncRead;
use futures::stream::MapErr;
use reqwest::Body;
use reqwest::Method;
use reqwest::Request;
use reqwest::Url;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;

pub struct HttpClient {
    client: reqwest::Client,
}

pub struct HttpRequest {
    pub method: HttpMethod,
    pub url: String,
    pub headers: HashMap<&'static str, String>,
    pub body: Option<String>,
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
    response: reqwest::Response,
}

type BytesResult = Result<Bytes, reqwest::Error>;
impl HttpResponse {
    pub fn lines(
        self,
    ) -> Lines<IntoAsyncRead<MapErr<impl Stream<Item = BytesResult>, impl FnMut(reqwest::Error) -> io::Error>>> {
        self.response
            .bytes_stream()
            .map_err(io::Error::other)
            .into_async_read()
            .lines()
    }

    pub async fn text(self) -> Result<String> {
        let body = self.response.text().await?;
        debug!(body, "[response]");
        Ok(body)
    }
}

impl HttpClient {
    pub async fn execute(&self, request: HttpRequest) -> Result<HttpResponse> {
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
                debug!(body, "[request]");
                *http_request.body_mut() = Some(Body::from(body));
            }

            let response = self.client.execute(http_request).await?;
            debug!(status = response.status().as_u16(), "[response]");
            for (key, value) in response.headers() {
                debug!("[header] {}={}", key, value.to_str()?);
            }

            Ok(HttpResponse { response })
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
