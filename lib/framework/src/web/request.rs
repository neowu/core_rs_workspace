use std::borrow::Cow;
use std::net::IpAddr;

use bytes::Bytes;
use http::HeaderMap;
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

use crate::exception::Exception;
use crate::exception::error_code;
use crate::json;
use crate::log::Severity;
use crate::web::X_FORWARDED_FOR;
use crate::web::X_FORWARDED_PROTO;

pub struct Request {
    parts: Parts,
    body: Option<Incoming>,
    client_ip: IpAddr,
    peer_addr: IpAddr,
    max_body_size: usize,
}

impl Request {
    /// Validates headers and parses client ip, rejects invalid request with `BAD_REQUEST`.
    pub(crate) fn parse(
        parts: Parts,
        body: Incoming,
        peer_addr: IpAddr,
        max_forwarded_ips: usize,
        max_body_size: usize,
    ) -> Result<Request, Exception> {
        validate_single_headers(&parts.headers)?;

        let client_ip = client_ip(&parts.headers, peer_addr, max_forwarded_ips)?;
        Ok(Request { parts, body: Some(body), client_ip, peer_addr, max_body_size })
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
    pub const fn client_ip(&self) -> IpAddr {
        self.client_ip
    }

    pub const fn peer_addr(&self) -> IpAddr {
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
        if contains_nul_escape(body) {
            return Err(exception!(
                "json body must not contain NUL char",
                severity = Severity::Warn,
                code = error_code::BAD_REQUEST
            ));
        }
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

// json escape allows "\u0000", NUL is rejected by postgres and truncates c string, serde_json already rejects lone surrogate
// raw control chars are invalid json, so NUL can only come from escape
fn contains_nul_escape(json: &str) -> bool {
    json.match_indices(r"\u0000").any(|(index, _)| {
        // odd number of consecutive backslashes means the last one starts the escape, otherwise it is an escaped "\\"
        json.as_bytes().iter().take(index + 1).rev().take_while(|&&byte| byte == b'\\').count() % 2 == 1
    })
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

// proxies append to the existing header, client may send the same header again, so the value read may not be the
// one proxy appended
fn validate_single_headers(headers: &HeaderMap) -> Result<(), Exception> {
    for name in [header::HOST, X_FORWARDED_FOR, X_FORWARDED_PROTO] {
        if headers.get_all(&name).iter().nth(1).is_some() {
            return Err(exception!(
                format!("duplicate header, name={name}"),
                severity = Severity::Warn,
                code = error_code::BAD_REQUEST
            ));
        }
    }
    Ok(())
}

fn client_ip(headers: &HeaderMap, peer_addr: IpAddr, max_forwarded_ips: usize) -> Result<IpAddr, Exception> {
    if max_forwarded_ips == 0 {
        return Ok(peer_addr);
    }
    let Some(value) = headers.get(X_FORWARDED_FOR) else {
        return Ok(peer_addr);
    };
    let x_forwarded_for = value.to_str().map_err(|err| {
        exception!(
            format!("invalid client ip address, value={value:?}"),
            severity = Severity::Warn,
            code = error_code::BAD_REQUEST,
            source = err
        )
    })?;
    if x_forwarded_for.trim().is_empty() {
        return Ok(peer_addr);
    }
    // x-forwarded-for = node, node, ..., take the node at max_forwarded_ips from right, values on the right are from trusted LB
    let node = x_forwarded_for.rsplit(',').take(max_forwarded_ips).last().unwrap_or_default().trim();
    extract_ip(node)
}

// node is ip or ipv4:port, only standard ip literals are accepted (no scope id, no legacy ipv4 forms like 1.2 or 010.1.1.1)
fn extract_ip(node: &str) -> Result<IpAddr, Exception> {
    // only 1 colon means ipv4:port, ipv6 has at least 2 colons
    let ip = match node.split_once(':') {
        Some((ip, port)) if !ip.is_empty() && !port.is_empty() && !port.contains(':') => ip,
        _ => node,
    };
    ip.parse::<IpAddr>().map_err(|err| {
        exception!(
            format!("invalid client ip address, value={node}"),
            severity = Severity::Warn,
            code = error_code::BAD_REQUEST,
            source = err
        )
    })
}

#[cfg(test)]
mod tests {
    use http::HeaderValue;

    use super::*;

    #[test]
    fn nul_escape() {
        assert!(contains_nul_escape(r#""a\u0000b""#));
        assert!(contains_nul_escape(r#"{"\u0000":"a"}"#));
        assert!(contains_nul_escape(r#""a\\\u0000""#));
        assert!(!contains_nul_escape(r#""a\\u0000""#));
        assert!(!contains_nul_escape(r#""a\u0001""#));
    }

    fn ip(value: &str) -> IpAddr {
        value.parse().unwrap()
    }

    fn client_ip_of(x_forwarded_for: &str, max_forwarded_ips: usize) -> Option<IpAddr> {
        let mut headers = HeaderMap::new();
        headers.insert(X_FORWARDED_FOR, x_forwarded_for.parse().unwrap());
        client_ip(&headers, ip("127.0.0.1"), max_forwarded_ips).ok()
    }

    #[test]
    fn client_ip_from_peer() {
        assert_eq!(client_ip(&HeaderMap::new(), ip("127.0.0.1"), 2).ok(), Some(ip("127.0.0.1")));
        assert_eq!(client_ip_of("", 2), Some(ip("127.0.0.1")));
        assert_eq!(client_ip_of("   ", 2), Some(ip("127.0.0.1")));
        assert_eq!(client_ip_of("108.0.0.1, 10.10.10.10", 0), Some(ip("127.0.0.1")));
        assert_eq!(client_ip_of("invalid", 0), Some(ip("127.0.0.1")));
    }

    #[test]
    fn client_ip_within_limits() {
        assert_eq!(client_ip_of("108.0.0.1", 2), Some(ip("108.0.0.1")));
        assert_eq!(client_ip_of(" 108.0.0.1 ", 2), Some(ip("108.0.0.1")));
        assert_eq!(client_ip_of("108.0.0.1, 10.10.10.10", 2), Some(ip("108.0.0.1")));
        assert_eq!(client_ip_of("2001:db8::1, 10.10.10.10", 2), Some(ip("2001:db8::1")));
    }

    #[test]
    fn client_ip_with_more_than_limits() {
        assert_eq!(client_ip_of("108.0.0.2, 108.0.0.1, 10.10.10.10", 2), Some(ip("108.0.0.1")));
        assert_eq!(client_ip_of("108.0.0.2, 108.0.0.1:5432, 10.10.10.10", 2), Some(ip("108.0.0.1")));
        assert_eq!(client_ip_of("invalid, 108.0.0.1, 10.10.10.10", 2), Some(ip("108.0.0.1")));
    }

    #[test]
    fn client_ip_rejects_invalid() {
        assert_eq!(client_ip_of("invalid", 2), None);
        assert_eq!(client_ip_of("1.2.3", 2), None);
        assert_eq!(client_ip_of("invalid, 10.10.10.10", 2), None);
        assert_eq!(client_ip_of(", 10.10.10.10", 2), None);

        let mut headers = HeaderMap::new();
        headers.insert(X_FORWARDED_FOR, HeaderValue::from_bytes("108.0.0.1, 中".as_bytes()).unwrap());
        let err = client_ip(&headers, ip("127.0.0.1"), 2).unwrap_err();
        assert_eq!(err.code, Some(error_code::BAD_REQUEST));
    }

    #[test]
    fn extract_ip_rejects_invalid() {
        assert_eq!(extract_ip("108.0.0.1").ok(), Some(ip("108.0.0.1")));
        assert_eq!(extract_ip("108.0.0.1:5432").ok(), Some(ip("108.0.0.1")));
        assert_eq!(extract_ip("2001:db8::1").ok(), Some(ip("2001:db8::1")));
        assert_eq!(extract_ip("::1").ok(), Some(ip("::1")));

        for value in [
            "dead",
            "::::",
            "ffff.ffff.ffff.ffff",
            "1.2",
            "192.0.2",
            "010.1.1.1",
            "256.1.1.1",
            "fe80::1%eth0",
            "[::1]:80",
            "108.0.0.1:",
            ":80",
            "localhost",
        ] {
            let err = extract_ip(value).unwrap_err();
            assert_eq!(err.code, Some(error_code::BAD_REQUEST), "value={value}");
        }
    }

    #[test]
    fn single_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, HeaderValue::from_static("localhost"));
        headers.insert(X_FORWARDED_FOR, HeaderValue::from_static("108.0.0.1, 10.0.0.1"));
        headers.append(header::ACCEPT, HeaderValue::from_static("text/html"));
        headers.append(header::ACCEPT, HeaderValue::from_static("application/json"));
        validate_single_headers(&headers).unwrap();

        for name in [header::HOST, X_FORWARDED_FOR, X_FORWARDED_PROTO] {
            let mut headers = headers.clone();
            headers.append(&name, HeaderValue::from_static("a"));
            headers.append(&name, HeaderValue::from_static("b"));
            let err = validate_single_headers(&headers).unwrap_err();
            assert_eq!(err.code, Some(error_code::BAD_REQUEST));
        }
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
