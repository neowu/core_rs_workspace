use std::borrow::Cow;
use std::net::SocketAddr;

use bytes::Bytes;
use framework::exception;
use framework::exception::Exception;
use framework::exception::error_code;
use framework::json;
use framework::log;
use framework::log::Severity;
use framework::warn;
use http::HeaderMap;
use http::HeaderName;
use http::Method;
use http::Uri;
use http::Version;
use http::header;
use http::header::AsHeaderName;
use http::request::Parts;
use http_body_util::BodyExt as _;
use http_body_util::Limited;
use hyper::body::Incoming;
use percent_encoding::percent_decode_str;
use serde::de::DeserializeOwned;

const X_FORWARDED_FOR: HeaderName = HeaderName::from_static("x-forwarded-for");

pub struct Request {
    parts: Parts,
    body: Option<Incoming>,
    client_ip: String,
    peer_addr: SocketAddr,
    max_body_size: usize,
}

impl Request {
    pub(crate) fn new(
        parts: Parts,
        body: Incoming,
        peer_addr: SocketAddr,
        max_forwarded_ips: usize,
        max_body_size: usize,
    ) -> Self {
        let client_ip = client_ip(&parts.headers, peer_addr, max_forwarded_ips);
        Self { parts, body: Some(body), client_ip, peer_addr, max_body_size }
    }

    pub const fn method(&self) -> &Method {
        &self.parts.method
    }

    pub const fn uri(&self) -> &Uri {
        &self.parts.uri
    }

    pub fn path(&self) -> &str {
        self.parts.uri.path()
    }

    pub fn query_string(&self) -> Option<&str> {
        self.parts.uri.query()
    }

    pub const fn version(&self) -> Version {
        self.parts.version
    }

    pub const fn headers(&self) -> &HeaderMap {
        &self.parts.headers
    }

    /// Header value as str, `None` if absent or not visible ascii.
    pub fn header(&self, name: impl AsHeaderName) -> Option<&str> {
        self.parts.headers.get(name)?.to_str().ok()
    }

    /// Percent decoded cookie value, h2 may split cookies into multiple headers.
    pub fn cookie(&self, name: &str) -> Option<Cow<'_, str>> {
        self.parts
            .headers
            .get_all(header::COOKIE)
            .iter()
            .filter_map(|value| value.to_str().ok())
            .flat_map(cookies)
            .find_map(|(key, value)| (key == name).then_some(value))
    }

    /// From `x-forwarded-for` within `max_forwarded_ips` hops, or the peer address.
    pub fn client_ip(&self) -> &str {
        &self.client_ip
    }

    pub const fn peer_addr(&self) -> SocketAddr {
        self.peer_addr
    }

    pub fn user_agent(&self) -> Option<&str> {
        self.header(header::USER_AGENT)
    }

    /// Parses query string, a missing query string is parsed as empty.
    pub fn query<T: DeserializeOwned>(&self) -> Result<T, Exception> {
        let query = self.parts.uri.query().unwrap_or_default();
        log!("[request] query={query}");
        serde_html_form::from_str(query).map_err(|err| {
            exception!("failed to parse query", severity = Severity::Warn, code = error_code::BAD_REQUEST, source = err)
        })
    }

    /// Reads the whole body, limited by `max_body_size`, the body can only be read once.
    pub async fn body(&mut self) -> Result<Bytes, Exception> {
        let body = self.body.take().ok_or_else(|| exception!("request body was already read"))?;
        let collected = Limited::new(body, self.max_body_size).collect().await.map_err(|err| {
            exception!(
                format!("failed to read body, error={err}"),
                severity = Severity::Warn,
                code = error_code::BAD_REQUEST
            )
        })?;
        Ok(collected.to_bytes())
    }

    /// Reads body as utf-8 text, logs the body.
    pub async fn text(&mut self) -> Result<String, Exception> {
        let body = self.body().await?;
        // reuses the buffer if the body arrived in one frame
        let body = String::from_utf8(Vec::from(body)).map_err(|err| {
            exception!("failed to read body", severity = Severity::Warn, code = error_code::BAD_REQUEST, source = err)
        })?;
        log!("[request] body={body}");
        Ok(body)
    }

    /// Reads and parses json body, logs the body.
    pub async fn json<T: DeserializeOwned>(&mut self) -> Result<T, Exception> {
        let body = self.body().await?;
        let body = str::from_utf8(&body).map_err(|err| {
            exception!("failed to read body", severity = Severity::Warn, code = error_code::BAD_REQUEST, source = err)
        })?;
        log!("[request] body={body}");
        json::from_json(body).map_err(|err| {
            exception!(
                "failed to parse json body",
                severity = Severity::Warn,
                code = error_code::BAD_REQUEST,
                source = err
            )
        })
    }
}

pub(crate) fn cookies(value: &str) -> impl Iterator<Item = (&str, Cow<'_, str>)> {
    value.split(';').filter_map(|pair| {
        let (name, raw_value) = pair.split_once('=')?;
        let name = name.trim();
        if name.is_empty() {
            return None;
        }
        let trimmed = raw_value.trim();
        let unquoted = trimmed.strip_prefix('"').and_then(|v| v.strip_suffix('"')).unwrap_or(trimmed);
        Some((name, percent_decode_str(unquoted).decode_utf8_lossy()))
    })
}

fn client_ip(headers: &HeaderMap, peer_addr: SocketAddr, max_forwarded_ips: usize) -> String {
    if max_forwarded_ips > 0
        && let Some(x_forwarded_for) = headers.get(X_FORWARDED_FOR).and_then(|value| value.to_str().ok())
        && let Some(client_ip) = extract_client_ip(x_forwarded_for, max_forwarded_ips)
    {
        return client_ip;
    }
    peer_addr.ip().to_string()
}

fn extract_client_ip(x_forwarded_for: &str, max_forwarded_ips: usize) -> Option<String> {
    if x_forwarded_for.trim().is_empty() {
        return None;
    }
    // x-forwarded-for = node, node, ..., take the node at max_forwarded_ips from right, values on the right are from trusted LB
    let node = x_forwarded_for.rsplit(',').take(max_forwarded_ips).last()?.trim();
    extract_ip(node)
}

// check loosely, ipv4 must have 3 dots and 1 optional colon (ipv4:port), ipv6 must have only colons, with hex chars
fn extract_ip(node: &str) -> Option<String> {
    let mut dots = 0;
    let mut last_dot_index = 0;
    let mut colons = 0;
    let mut last_colon_index = 0;

    for (i, ch) in node.bytes().enumerate() {
        if ch == b'.' {
            dots += 1;
            last_dot_index = i;
        } else if ch == b':' {
            colons += 1;
            last_colon_index = i;
        } else if !ch.is_ascii_hexdigit() {
            warn!(error_code = "BAD_REQUEST", "invalid character in client ip address, value={node}");
            return None;
        }
    }

    if dots == 0 || (dots == 3 && colons == 0) {
        return Some(node.to_owned());
    }
    if dots == 3 && colons == 1 && last_colon_index > last_dot_index && last_colon_index < node.len() - 1 {
        return node.get(..last_colon_index).map(str::to_owned);
    }
    warn!(error_code = "BAD_REQUEST", "invalid client ip address, value={node}");
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_ip_from_headers() {
        let peer_addr: SocketAddr = "127.0.0.1:1234".parse().unwrap();
        let mut headers = HeaderMap::new();
        assert_eq!(client_ip(&headers, peer_addr, 2), "127.0.0.1");

        headers.insert(X_FORWARDED_FOR, "invalid".parse().unwrap());
        assert_eq!(client_ip(&headers, peer_addr, 2), "127.0.0.1");

        headers.insert(X_FORWARDED_FOR, "1.2.3".parse().unwrap());
        assert_eq!(client_ip(&headers, peer_addr, 2), "127.0.0.1");

        headers.insert(X_FORWARDED_FOR, "108.0.0.1, 10.10.10.10".parse().unwrap());
        assert_eq!(client_ip(&headers, peer_addr, 2), "108.0.0.1");
        assert_eq!(client_ip(&headers, peer_addr, 0), "127.0.0.1");
    }

    #[test]
    fn extract_client_ip_with_empty() {
        assert_eq!(extract_client_ip("", 2), None);
        assert_eq!(extract_client_ip("   ", 2), None);
    }

    #[test]
    fn extract_client_ip_within_limits() {
        assert_eq!(extract_client_ip("108.0.0.1", 2), Some("108.0.0.1".to_owned()));
        assert_eq!(extract_client_ip(" 108.0.0.1 ", 2), Some("108.0.0.1".to_owned()));
        assert_eq!(extract_client_ip("108.0.0.1, 10.10.10.10", 2), Some("108.0.0.1".to_owned()));
        assert_eq!(extract_client_ip("2001:db8::1, 10.10.10.10", 2), Some("2001:db8::1".to_owned()));
    }

    #[test]
    fn extract_client_ip_with_more_than_limits() {
        assert_eq!(extract_client_ip("108.0.0.2, 108.0.0.1, 10.10.10.10", 2), Some("108.0.0.1".to_owned()));
        assert_eq!(extract_client_ip("108.0.0.2, 108.0.0.1:5432, 10.10.10.10", 2), Some("108.0.0.1".to_owned()));
    }

    #[test]
    fn parse_cookies() {
        let parsed: Vec<_> = cookies("a=1; b=\"hello%20world\"; ;c=; =x; d").collect();
        assert_eq!(
            parsed,
            vec![("a", Cow::Borrowed("1")), ("b", Cow::Borrowed("hello world")), ("c", Cow::Borrowed(""))]
        );
    }
}
