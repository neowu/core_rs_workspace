# HTTP client

Code: [`lib/framework/src/http.rs`](../lib/framework/src/http.rs),
[`http/dns.rs`](../lib/framework/src/http/dns.rs) · also used by:
[clickhouse](clickhouse.md)

## Fallback DNS cache

`HttpClientConfig::enable_fallback_dns_cache` (default off) replaces reqwest's resolver with
`FallbackDnsResolver`:

- Every connect still resolves through DNS (`getaddrinfo`, same as the default resolver) and
  remembers the result per host.
- Only when the lookup fails are the last good addrs reused, with a `DNS_RESOLVE_FAILED` warning.
  A successful lookup always replaces them, so an IP change is picked up as soon as DNS answers.
- A host never resolved since process start has no fallback; the error is returned as is.

Motivation: a GKE control-plane upgrade dropped service DNS records for ~14s, and the Cloud Run
resolver cached the NXDOMAIN for the zone's SOA negative TTL (300s), so Cloud Run clients failed
for 5 minutes while the services were up. It is meant for targets with a stable IP (e.g. a GKE
service IP); for a headless service the fallback addrs may be stale pod IPs.

It implements both reqwest's `Resolve` and hyper's resolver `Service<Name>`, so it plugs into
`reqwest::ClientBuilder::dns_resolver` and `HttpConnector::new_with_resolver` (any hyper based
client, e.g. `framework_clickhouse`) without an adapter.
