use std::fmt::Debug;
use std::fs;
use std::io;
use std::mem;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use bytes::Bytes;
use futures::Stream as _;
use http::HeaderMap;
use http::HeaderName;
use http::HeaderValue;
use http::StatusCode;
use http::header;
use http_body::Frame;
use http_body::SizeHint;
use tokio::fs::File;
use tokio::io::AsyncReadExt as _;
use tokio::io::Take;
use tokio_util::io::ReaderStream;

use crate::api::ErrorResponse;
use crate::exception::Exception;
use crate::exception::error_code;
use crate::json;

const APPLICATION_JSON: HeaderValue = HeaderValue::from_static("application/json");
const TEXT_PLAIN: HeaderValue = HeaderValue::from_static("text/plain; charset=utf-8");
const TEXT_HTML: HeaderValue = HeaderValue::from_static("text/html; charset=utf-8");

pub struct Response {
    status: StatusCode,
    headers: HeaderMap,
    body: Body,
}

impl Response {
    /// 204 with no body.
    pub fn empty() -> Self {
        Self { status: StatusCode::NO_CONTENT, headers: HeaderMap::new(), body: Body::Empty }
    }

    /// 200 with `application/json` body, logs the body.
    pub fn json<T>(value: &T) -> Result<Self, Exception>
    where
        T: serde::Serialize + Debug,
    {
        let body = json::to_json(value)?;
        log!("[response] body={body}");
        stats!(response_content_length = body.len());
        Ok(Self::bytes(body, APPLICATION_JSON))
    }

    /// 200 with `text/plain; charset=utf-8` body.
    pub fn text(body: impl Into<Bytes>) -> Self {
        Self::bytes(body, TEXT_PLAIN)
    }

    /// 200 with `text/html; charset=utf-8` body.
    pub fn html(body: impl Into<Bytes>) -> Self {
        Self::bytes(body, TEXT_HTML)
    }

    /// 200 with body of given content type.
    pub fn bytes(body: impl Into<Bytes>, content_type: HeaderValue) -> Self {
        let mut headers = HeaderMap::with_capacity(1);
        headers.insert(header::CONTENT_TYPE, content_type);
        Self { status: StatusCode::OK, headers, body: Body::Full(body.into()) }
    }

    #[must_use]
    pub const fn status(mut self, status: StatusCode) -> Self {
        self.status = status;
        self
    }

    #[must_use]
    pub fn header(mut self, name: HeaderName, value: HeaderValue) -> Self {
        self.headers.insert(name, value);
        self
    }

    pub const fn status_code(&self) -> StatusCode {
        self.status
    }

    pub const fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    pub const fn headers_mut(&mut self) -> &mut HeaderMap {
        &mut self.headers
    }

    pub(crate) fn file(file: fs::File, length: u64) -> Self {
        // limit to the stat length, a growing file must not exceed content-length
        let stream = ReaderStream::with_capacity(File::from_std(file).take(length), 64 * 1024);
        Self { status: StatusCode::OK, headers: HeaderMap::new(), body: Body::File { stream, remaining: length } }
    }

    // json body without logging, the exception is already logged with the action
    pub(crate) fn error(exception: &Exception) -> Self {
        let status = exception.code.map_or(StatusCode::INTERNAL_SERVER_ERROR, |code| match code {
            error_code::BAD_REQUEST | error_code::VALIDATION_ERROR => StatusCode::BAD_REQUEST,
            error_code::NOT_FOUND => StatusCode::NOT_FOUND,
            error_code::FORBIDDEN => StatusCode::FORBIDDEN,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        });
        let body = ErrorResponse {
            severity: exception.severity,
            code: exception.code.map(str::to_owned),
            message: exception.message.clone(),
        };
        match json::to_json(&body) {
            Ok(body) => Self::bytes(body, APPLICATION_JSON).status(status),
            Err(_) => Self::empty().status(status),
        }
    }

    // hyper h2 still sends the body of HEAD responses, which h2 clients reject, so keep content-length and drop the body
    pub(crate) fn without_body(mut self) -> Self {
        if let Some(length) = http_body::Body::size_hint(&self.body).exact()
            && length > 0
            && !self.headers.contains_key(header::CONTENT_LENGTH)
        {
            self.headers.insert(header::CONTENT_LENGTH, HeaderValue::from(length));
        }
        self.body = Body::Empty;
        self
    }

    pub(crate) fn without_content(mut self) -> Self {
        self.headers.remove(header::CONTENT_LENGTH);
        self.body = Body::Empty;
        self
    }

    pub(crate) fn into_http(self) -> http::Response<Body> {
        let mut response = http::Response::new(self.body);
        *response.status_mut() = self.status;
        *response.headers_mut() = self.headers;
        response
    }
}

// own body type instead of BoxBody, saves one allocation and dynamic dispatch per response
pub(crate) enum Body {
    Empty,
    Full(Bytes),
    File { stream: ReaderStream<Take<File>>, remaining: u64 },
}

impl http_body::Body for Body {
    type Data = Bytes;
    type Error = io::Error;

    fn poll_frame(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Bytes>, io::Error>>> {
        let this = self.get_mut();
        match this {
            Body::Empty => Poll::Ready(None),
            Body::Full(_) => {
                let Body::Full(bytes) = mem::replace(this, Body::Empty) else { return Poll::Ready(None) };
                if bytes.is_empty() { Poll::Ready(None) } else { Poll::Ready(Some(Ok(Frame::data(bytes)))) }
            }
            Body::File { stream, remaining } => match Pin::new(stream).poll_next(cx) {
                Poll::Ready(Some(Ok(bytes))) => {
                    *remaining = remaining.saturating_sub(bytes.len() as u64);
                    Poll::Ready(Some(Ok(Frame::data(bytes))))
                }
                Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err))),
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            },
        }
    }

    fn is_end_stream(&self) -> bool {
        match self {
            Body::Empty => true,
            Body::Full(bytes) => bytes.is_empty(),
            Body::File { remaining, .. } => *remaining == 0,
        }
    }

    fn size_hint(&self) -> SizeHint {
        match self {
            Body::Empty => SizeHint::with_exact(0),
            Body::Full(bytes) => SizeHint::with_exact(bytes.len() as u64),
            Body::File { remaining, .. } => SizeHint::with_exact(*remaining),
        }
    }
}

#[cfg(test)]
mod tests {
    use http_body_util::BodyExt as _;

    use super::*;
    use crate::log::Severity;

    #[tokio::test]
    async fn json() {
        let response = Response::json(&vec!["a", "b"]).unwrap();
        assert_eq!(response.status_code(), StatusCode::OK);
        assert_eq!(response.headers().get(header::CONTENT_TYPE), Some(&APPLICATION_JSON));
        let body = response.into_http().into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body, r#"["a","b"]"#);
    }

    #[test]
    fn empty() {
        let response = Response::empty().header(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
        assert_eq!(response.status_code(), StatusCode::NO_CONTENT);
        assert_eq!(http_body::Body::size_hint(&response.body).exact(), Some(0));
    }

    #[tokio::test]
    async fn error() {
        let response = Response::error(&exception!(
            "invalid name",
            severity = Severity::Warn,
            code = error_code::VALIDATION_ERROR
        ));
        assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
        let body = response.into_http().into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body, r#"{"severity":"WARN","code":"VALIDATION_ERROR","message":"invalid name"}"#);

        let internal_error = Response::error(&exception!("failed"));
        assert_eq!(internal_error.status_code(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}
