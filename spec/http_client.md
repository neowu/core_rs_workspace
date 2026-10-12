# HTTP client

Code: [`lib/framework/src/http.rs`](../lib/framework/src/http.rs),
[`http/dns.rs`](../lib/framework/src/http/dns.rs) · also used by:
[clickhouse](clickhouse.md)

## Transport

Built directly on `hyper::client::conn` (no reqwest / tower), TLS via `tokio-rustls` (aws-lc-rs).

- `HttpClient` is not `Clone`; it owns its pool, share it via `Arc<HttpClient>` (e.g. `#[api]` clients take one).
- Connection pool keyed by `scheme://host:port`. HTTP/1 connections go back to the pool only after
  the response body is fully read; an SSE connection is never pooled and closes when the stream ends.
  One HTTP/2 connection is shared by all concurrent requests to the same key.
- Idle connections expire after 90s (same as reqwest default); closed ones are dropped on checkout/release.
  At most 32 idle connections per key, the oldest is evicted, to bound sockets kept after an HTTP/1 burst.
  No background sweep: idle connections of a key that is no longer used stay until the peer closes them.
- TCP keepalive (15s idle, 15s interval, 3 probes, same as reqwest) so a pooled connection silently
  dropped by a LB / NAT is detected as closed instead of hanging the next request until timeout.
- A request on a pooled connection that the peer closed before it was sent (`is_canceled`) is
  resent once on a new connection, independent of `RetryConfig`.
- Protocol: https negotiates `h2` / `http/1.1` via ALPN; plain http uses h2c only with
  `prefer_http2`, otherwise HTTP/1.
- Certs: platform verifier (`rustls-platform-verifier`) by default; `accept_certs` trusts only the
  given certs; `accept_invalid_cert` skips verification.
- `timeout` covers connect, request and reading the body of `execute`; for `sse` it only covers until
  response headers.
- No redirects, no proxy env vars, no decompression; urls must be valid `http::Uri` (already encoded),
  an explicit port must be a valid u16 (`http::Uri` doesn't check it); the url is parsed in
  `HttpRequest::new`, which returns an error for an invalid url, so it fails before sending.

## SSE

`sse` returns `EventSource`, a stream of `web::Event` (shared with the server), parsed per the WHATWG
event stream format so it reads any server, not only ours:

- Lines end with CRLF / LF / CR (also split across chunks); `field:value`, one optional leading space
  stripped; `:` lines are comments (keepalive); unknown fields and `retry` are ignored.
- `data` lines are joined with `\n`, an event is dispatched on a blank line, none if there was no `data`
  line; an incomplete event at the end of the stream is discarded.
- `event_type` resets per event; `id` is the last event id, it persists across events (an empty `id`
  clears it), the value to send as `last-event-id` on reconnect. No auto reconnect.

## Fallback DNS cache

`HttpClientConfig::enable_fallback_dns_cache` (default off) resolves through
`FallbackDnsResolver` instead of the system resolver:

- Every connect still resolves through DNS (`getaddrinfo`, same as the default resolver) and
  remembers the result per host.
- Only when the lookup fails are the last good addrs reused, with a `DNS_RESOLVE_FAILED` warning.
  A successful lookup always replaces them, so an IP change is picked up as soon as DNS answers.
- Fallback addrs expire 1 hour after the last successful lookup; an expired entry is removed lazily
  when a failed lookup finds it, and the error is returned as is.
- A host never resolved since process start has no fallback; the error is returned as is.
- All resolved addrs are kept (not just the first), so the connector can still try the next addr /
  the other IP family; memory is a few `SocketAddr`s per host.

Motivation: a GKE control-plane upgrade dropped service DNS records for ~14s, and the Cloud Run
resolver cached the NXDOMAIN for the zone's SOA negative TTL (300s), so Cloud Run clients failed
for 5 minutes while the services were up. It is meant for targets with a stable IP (e.g. a GKE
service IP); for a headless service the fallback addrs may be stale pod IPs.

`lookup` is public so other clients can reuse it: `framework_clickhouse` wraps it in a hyper-util
resolver `Service<Name>` (the `clickhouse` crate only accepts a hyper-util legacy client), which keeps
`tower-service` and hyper-util `client-legacy` out of `framework`.
