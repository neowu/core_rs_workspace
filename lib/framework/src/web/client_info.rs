use std::net::SocketAddr;

use axum::extract::ConnectInfo;
use axum::extract::Request;
use axum::http::HeaderName;
use axum::http::header;
use tracing::warn;

pub(crate) const X_FORWARDED_FOR: HeaderName = HeaderName::from_static("x-forwarded-for");

#[derive(Debug)]
pub struct ClientInfo {
    pub client_ip: String,
    pub user_agent: Option<String>,
}

pub(crate) fn client_info(request: &Request, max_forwarded_ips: usize) -> ClientInfo {
    let user_agent =
        request.headers().get(header::USER_AGENT).map(|value| value.to_str().unwrap_or_default().to_owned());

    let mut client_ip: Option<String> = None;
    if max_forwarded_ips > 0
        && let Some(x_forwarded_for) = request.headers().get(X_FORWARDED_FOR)
    {
        client_ip = extract_client_ip(x_forwarded_for.to_str().unwrap_or_default(), max_forwarded_ips);
    }

    if client_ip.is_none()
        && let Some(connect_info) = request.extensions().get::<ConnectInfo<SocketAddr>>()
    {
        client_ip = Some(connect_info.0.ip().to_string());
    }

    ClientInfo { client_ip: client_ip.unwrap_or("unknown".to_owned()), user_agent }
}

fn extract_client_ip(x_forwarded_for: &str, max_forwarded_ips: usize) -> Option<String> {
    if x_forwarded_for.trim().is_empty() {
        return None;
    }

    let mut found_forwarded_ips = 1;
    let mut start: usize = 0;
    let mut end = x_forwarded_for.len();

    for (i, ch) in x_forwarded_for.bytes().enumerate().rev() {
        if ch as char == ',' {
            found_forwarded_ips += 1;
            if found_forwarded_ips > max_forwarded_ips {
                start = i + 1;
                break;
            }
            end = i;
        }
    }

    // According to https://tools.ietf.org/html/rfc7239
    // x-forwarded-for = node, node, ...
    // node     = nodename [ ":" node-port ]
    // nodename = IPv4address / "[" IPv6address "]" / "unknown" / obfnode
    // Currently only Azure Application Gateway may use ipv4:port, and it doesn't support ipv6 yet
    // So here only to support ipv4, ipv4:port, ipv6 format
    let node = x_forwarded_for[start..end].trim();
    extract_ip(node)
}

// Check loosely to avoid unnecessary overhead, especially x-forwarded-for is extracted from right to left, where values are from trusted LB
// ipv4 must have 3 dots and 1 optional colon, with hex chars
// ipv6 must have only colons with hex chars
fn extract_ip(node: &str) -> Option<String> {
    let length = node.len();
    let mut dots = 0;
    let mut last_dot_index: Option<usize> = None;
    let mut colons = 0;
    let mut last_colon_index: Option<usize> = None;

    for (i, ch) in node.chars().enumerate() {
        if ch == '.' {
            dots += 1;
            last_dot_index = Some(i);
        } else if ch == ':' {
            colons += 1;
            last_colon_index = Some(i);
        } else if ch.to_digit(16).is_none() {
            // Invalid character in IP address
            warn!("Invalid character in client IP address: {}", node);
            return None;
        }
    }

    if dots == 0 {
        return Some(node.to_owned()); // Should be ipv6 format
    }

    if dots == 3 && colons == 0 {
        // Should be ipv4 format
        return Some(node.to_owned());
    }

    if dots == 3
        && colons == 1
        && let Some(last_dot_index) = last_dot_index
        && let Some(last_colon_index) = last_colon_index
        && last_colon_index > last_dot_index
        && last_colon_index < length - 1
    {
        // Should be ipv4:port
        return Some(node[0..last_colon_index].to_string());
    }

    warn!("Invalid client IP address: {node}");
    None
}

#[cfg(test)]
mod tests {
    use super::*;

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
    }

    #[test]
    fn extract_client_ip_with_more_than_limits() {
        assert_eq!(extract_client_ip("108.0.0.2, 108.0.0.1, 10.10.10.10", 2), Some("108.0.0.1".to_owned()));
        assert_eq!(extract_client_ip("108.0.0.2, 108.0.0.1:5432, 10.10.10.10", 2), Some("108.0.0.1".to_owned()));
    }
}
