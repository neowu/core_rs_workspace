use std::net::IpAddr;

use crate::exception::Exception;
use crate::exception::error_code;
use crate::log::Severity;
use crate::web::request::Request;

/// Rejects with 403 unless the peer address is loopback or private network, `x-forwarded-for` is ignored.
pub fn validate_local_peer(request: &Request) -> Result<(), Exception> {
    if is_local(request.peer_addr()) {
        return Ok(());
    }
    Err(exception!("access denied", severity = Severity::Warn, code = error_code::FORBIDDEN))
}

pub const fn is_local(ip: IpAddr) -> bool {
    // dual stack listener reports ipv4 peer as ipv4-mapped ipv6
    match ip.to_canonical() {
        IpAddr::V4(ip) => ip.is_loopback() || ip.is_private(),
        IpAddr::V6(ip) => ip.is_loopback() || ip.is_unique_local(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_ip() {
        assert!(is_local("127.0.0.1".parse().unwrap()));
        assert!(is_local("10.1.2.3".parse().unwrap()));
        assert!(is_local("172.16.0.1".parse().unwrap()));
        assert!(is_local("192.168.1.1".parse().unwrap()));
        assert!(is_local("::1".parse().unwrap()));
        assert!(is_local("fd00::1".parse().unwrap()));
        assert!(is_local("::ffff:10.0.0.1".parse().unwrap()));

        assert!(!is_local("8.8.8.8".parse().unwrap()));
        assert!(!is_local("172.32.0.1".parse().unwrap()));
        assert!(!is_local("2001:db8::1".parse().unwrap()));
        assert!(!is_local("::ffff:8.8.8.8".parse().unwrap()));
    }
}
