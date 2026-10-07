use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::net::ToSocketAddrs as _;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;
use std::vec;

use hyper_util::client::legacy::connect::dns::Name;
use reqwest::dns::Addrs;
use reqwest::dns::Name as ReqwestName;
use reqwest::dns::Resolve;
use reqwest::dns::Resolving;
use tokio::task;
use tower_service::Service;

use crate::warn;

// always resolves via dns first, only falls back to the last resolved addrs when dns fails,
// e.g. GKE dns briefly drops service records and the cloud run resolver caches NXDOMAIN for the SOA ttl (300s)
#[derive(Clone, Default)]
pub struct FallbackDnsResolver {
    resolved: Arc<Mutex<HashMap<String, Vec<SocketAddr>>>>,
}

impl FallbackDnsResolver {
    // port is 0, the http client replaces it with the port of the url, same as getaddrinfo resolver of hyper
    async fn lookup(&self, host: &str) -> Result<Vec<SocketAddr>, io::Error> {
        let name = host.to_owned();
        let result = task::spawn_blocking(move || (name.as_str(), 0).to_socket_addrs().map(Iterator::collect))
            .await
            .map_err(io::Error::other)
            .flatten();
        match result {
            Ok(addrs) => {
                self.resolved.lock().unwrap().insert(host.to_owned(), Vec::clone(&addrs));
                Ok(addrs)
            }
            Err(err) => {
                let Some(addrs) = self.resolved.lock().unwrap().get(host).cloned() else {
                    return Err(err);
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

// for reqwest
impl Resolve for FallbackDnsResolver {
    fn resolve(&self, name: ReqwestName) -> Resolving {
        let resolver = self.clone();
        Box::pin(async move {
            let addrs = resolver.lookup(name.as_str()).await?;
            Ok(Box::new(addrs.into_iter()) as Addrs)
        })
    }
}

// for hyper HttpConnector::new_with_resolver
impl Service<Name> for FallbackDnsResolver {
    type Response = vec::IntoIter<SocketAddr>;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, name: Name) -> Self::Future {
        let resolver = self.clone();
        Box::pin(async move { Ok(resolver.lookup(name.as_str()).await?.into_iter()) })
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
        resolver.resolved.lock().unwrap().insert(host.to_owned(), vec![addr]);
        assert_eq!(resolver.lookup(host).await.unwrap(), vec![addr]);
    }

    #[tokio::test]
    async fn remember_resolved() {
        let resolver = FallbackDnsResolver::default();
        let addrs = resolver.lookup("localhost").await.unwrap();
        assert!(!addrs.is_empty());
        assert_eq!(resolver.resolved.lock().unwrap().get("localhost"), Some(&addrs));
    }
}
