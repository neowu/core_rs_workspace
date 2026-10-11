use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::net::ToSocketAddrs as _;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use tokio::task;

use crate::warn;

// always resolves via dns first, only falls back to the last resolved addrs when dns fails,
// e.g. GKE dns briefly drops service records and the cloud run resolver caches NXDOMAIN for the SOA ttl (300s)
#[derive(Clone, Default)]
pub struct FallbackDnsResolver {
    resolved: Arc<Mutex<HashMap<String, DnsRecord>>>,
}

struct DnsRecord {
    addrs: Vec<SocketAddr>,
    resolved_at: Instant,
}

const TTL: Duration = Duration::from_hours(1);

impl FallbackDnsResolver {
    // port is 0, the http client replaces it with the port of the url, same as getaddrinfo resolver of hyper
    pub async fn lookup(&self, host: &str) -> Result<Vec<SocketAddr>, io::Error> {
        let name = host.to_owned();
        let result = task::spawn_blocking(move || (name.as_str(), 0).to_socket_addrs().map(Iterator::collect))
            .await
            .map_err(io::Error::other)
            .flatten();
        match result {
            Ok(addrs) => {
                self.resolved
                    .lock()
                    .unwrap()
                    .insert(host.to_owned(), DnsRecord { addrs: Vec::clone(&addrs), resolved_at: Instant::now() });
                Ok(addrs)
            }
            Err(err) => {
                let addrs = {
                    let mut resolved = self.resolved.lock().unwrap();
                    match resolved.get(host) {
                        Some(entry) if entry.resolved_at.elapsed() < TTL => entry.addrs.clone(),
                        Some(_) => {
                            resolved.remove(host);
                            return Err(err);
                        }
                        None => return Err(err),
                    }
                };
                warn!(
                    error_code = "DNS_RESOLVE_FAILED",
                    "failed to resolve host, fallback to last resolved addrs, host={host}, addrs={addrs:?}, error={err}"
                );
                Ok(addrs)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn fallback_to_last_resolved() {
        let resolver = FallbackDnsResolver::default();
        // .invalid is reserved to never resolve, RFC 6761
        let host = "service.invalid";
        resolver.lookup(host).await.unwrap_err();

        let addr: SocketAddr = "10.0.0.1:0".parse().unwrap();
        resolver
            .resolved
            .lock()
            .unwrap()
            .insert(host.to_owned(), DnsRecord { addrs: vec![addr], resolved_at: Instant::now() });
        assert_eq!(resolver.lookup(host).await.unwrap(), vec![addr]);
    }

    #[tokio::test]
    async fn expired_fallback_removed() {
        let resolver = FallbackDnsResolver::default();
        let host = "service.invalid";
        let addr: SocketAddr = "10.0.0.1:0".parse().unwrap();
        resolver.resolved.lock().unwrap().insert(
            host.to_owned(),
            DnsRecord { addrs: vec![addr], resolved_at: Instant::now().checked_sub(TTL).unwrap() },
        );

        resolver.lookup(host).await.unwrap_err();
        assert!(resolver.resolved.lock().unwrap().get(host).is_none());
    }

    #[tokio::test]
    async fn remember_resolved() {
        let resolver = FallbackDnsResolver::default();
        let addrs = resolver.lookup("localhost").await.unwrap();
        assert!(!addrs.is_empty());
        assert_eq!(resolver.resolved.lock().unwrap().get("localhost").map(|entry| &entry.addrs), Some(&addrs));
    }
}
