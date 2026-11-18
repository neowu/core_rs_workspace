use std::net::IpAddr;

use crate::exception::Exception;
use crate::exception::error_code;
use crate::log::Severity;
use crate::web::X_FORWARDED_FOR;
use crate::web::request::Request;

/// For operation endpoints (`/_sys/`), called by each controller, not checked by the server on every request.
/// Rejects with 403 unless the peer address is loopback (same host, or kubectl port-forward which connects inside
/// the pod network namespace) and the request was not forwarded,
/// a proxy / load balancer always appends `x-forwarded-for`, so its presence means the request is not a direct call.
pub fn validate_local_peer(request: &Request) -> Result<(), Exception> {
    if is_loopback(request.peer_addr()) && !request.headers().contains_key(X_FORWARDED_FOR) {
        return Ok(());
    }
    Err(exception!("access denied", severity = Severity::Warn, code = error_code::FORBIDDEN))
}

const fn is_loopback(ip: IpAddr) -> bool {
    // dual stack listener reports ipv4 peer as ipv4-mapped ipv6
    ip.to_canonical().is_loopback()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_ip() {
        assert!(is_loopback("127.0.0.1".parse().unwrap()));
        assert!(is_loopback("127.1.2.3".parse().unwrap()));
        assert!(is_loopback("::1".parse().unwrap()));
        assert!(is_loopback("::ffff:127.0.0.1".parse().unwrap()));

        assert!(!is_loopback("10.1.2.3".parse().unwrap()));
        assert!(!is_loopback("172.16.0.1".parse().unwrap()));
        assert!(!is_loopback("192.168.1.1".parse().unwrap()));
        assert!(!is_loopback("fd00::1".parse().unwrap()));
        assert!(!is_loopback("::ffff:10.0.0.1".parse().unwrap()));
        assert!(!is_loopback("8.8.8.8".parse().unwrap()));
        assert!(!is_loopback("::".parse().unwrap()));
    }
}
