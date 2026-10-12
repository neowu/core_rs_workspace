# HTTP worktree review

Reviewed on 2026-10-10 against `HEAD`, including the Hyper client replacement, SSE changes,
generated API clients, ClickHouse resolver adapter, dependencies, tests, and specs.
These are proposed changes; application code has not been modified by this review.

## Findings

### P1: Actually expire idle connections and bound pool retention

Code: [checkout](../lib/framework/src/http.rs#L357), [release](../lib/framework/src/http.rs#L379).

The 90-second timeout is checked only when the same origin is checked out or released. After
a burst finishes and that origin is no longer used, its senders remain in the pool indefinitely.
For peers that keep connections open, the spawned connection drivers and sockets remain alive
too; TCP keepalive checks peer liveness rather than enforcing an idle lifetime. A client serving
many origins or a large HTTP/1 burst can retain enough sockets to exhaust file descriptors.
Empty origin entries also remain after checkout removes their last connection.

Add cleanup that runs even without another request to that origin, remove empty entries, and
bound retained idle connections per origin (and total origins if URLs are dynamic). This would
make the [documented idle expiry](../spec/http_client.md#transport) true in practice.

Validation: issue a concurrent HTTP/1 burst against a peer that keeps sockets open, stop making
requests, and verify that idle sockets close after the configured lifetime without a checkout.
Also verify that the retention cap survives a large burst and origin churn.

### P1: Preserve fallback between reachable IP families

Code: [connect](../lib/framework/src/http.rs#L394).

Both connection paths now use `TcpStream::connect`, which tries resolved addresses sequentially.
If the first address silently drops SYN packets, its connect attempt can consume the entire
request timeout before the next address is attempted. A dual-stack host with unreachable IPv6
and working IPv4 can therefore fail every request despite being reachable. Configured retries
repeat the same ordering. The previous connector had Happy Eyeballs fallback.

Use staggered attempts across address families under the existing overall deadline, canceling
losing attempts after one succeeds. Preserve all DNS fallback addresses. The sequential behavior
is visible in [Tokio's connection implementation](https://docs.rs/tokio/latest/src/tokio/net/tcp/stream.rs.html);
the previous [Hyper-util connector](https://docs.rs/hyper-util/latest/src/hyper_util/client/legacy/connect/http.rs.html)
starts the alternate family after a default 300ms delay.

Validation: resolve to a blackholed preferred-family address followed by a reachable alternate
family and check that the request succeeds before its overall timeout, with fallback DNS both
enabled and disabled.

### P2: Coordinate concurrent HTTP/2 connection establishment

Code: [send](../lib/framework/src/http.rs#L337), [HTTP/2 publication](../lib/framework/src/http.rs#L347),
[checkout](../lib/framework/src/http.rs#L369).

Concurrent requests to a cold origin all observe an empty pool and independently await
DNS/TCP/TLS/handshake. Each then publishes a different HTTP/2 sender; there is no pending-connect
coordination or second check before publication. For example, 50 simultaneous first requests can
open 50 connections instead of multiplexing on one. Subsequent requests take the newest sender,
leaving the other connections retained until cleanup. This contradicts the spec's one shared
HTTP/2 connection per origin and wastes handshakes, sockets, and server connection state.

Coordinate establishment per origin for h2c and consolidate connections once HTTPS negotiates
HTTP/2. Keep the shared HTTP/2 sender available for cloning instead of treating it as an
exclusive idle HTTP/1 connection. Do not serialize requests after establishment.

Validation: start simultaneous requests before any warmup and count accepted connections, then
verify warmed concurrent requests use the same connection. Cover h2c and HTTPS with ALPN.

### P2: Reject explicit invalid ports instead of using the default

Code: [Target::parse](../lib/framework/src/http.rs#L175), especially line 189.

`http::Uri` validates URI syntax but does not guarantee that an authority's port is a valid
`u16`. `port_u16()` returns `None` for both an absent port and an invalid port. Consequently,
`http://example.com:99999/path` falls through to port 80 while the original invalid authority is
still used for `Host` or `:authority`. The client can send a request to a different port than
the caller specified instead of rejecting the URL, and shares its pool key with valid port-80
requests. The same problem applies to nonnumeric ports.

Distinguish an absent port from an explicit invalid port and reject the latter before connecting.
Account for IPv6 brackets when identifying the port suffix. This distinction follows from the
[authority implementation](https://docs.rs/http/latest/src/http/uri/authority.rs.html), which
discards failed port parsing in `port()`.

Validation: accept omitted ports and valid explicit ports; reject `:99999` and `:abc` for both
schemes, including bracketed IPv6 authorities, without opening a socket.

### P2: Strip the leading UTF-8 BOM from SSE streams

Code: [EventSource::read](../lib/framework/src/http.rs#L557),
[process_line](../lib/framework/src/http.rs#L573).

The parser decodes each line but never strips the stream's leading BOM. For the valid stream
`\u{FEFF}data: hello\n\n`, the first field becomes `\u{FEFF}data`, is treated as unknown, and the
event is lost. A BOM before `id:` instead loses the reconnect ID. The client spec now promises
WHATWG parsing of third-party streams, where a leading BOM is permitted and must be ignored.
See the [WHATWG decoding requirement](https://html.spec.whatwg.org/multipage/server-sent-events.html#event-stream-interpretation).

Strip exactly one BOM at the beginning of the stream, including when its bytes span chunks;
preserve BOM characters elsewhere in event data.

Validation: cover a BOM before `data:` and `id:`, a BOM split across byte chunks, and a BOM inside
data that must remain unchanged.

## Performance improvements

- **Avoid scanning the whole idle vector on every return.**
  [release](../lib/framework/src/http.rs#L379) retains every connection while holding one mutex
  shared by all origins. Returning N connections from a burst visits roughly N(N-1)/2 entries
  while they are all still live. HTTP/2 checkout also invokes this scan on every request.
  Bound the vectors, move expiry work to cleanup, and store the shared HTTP/2 sender separately.
  Measure burst completion latency and mutex contention across multiple origins.
- **Reuse SSE line-buffer capacity.**
  [read](../lib/framework/src/http.rs#L564) takes and drops the vector after every line, so the
  next nonempty line allocates again. Clear and restore the buffer after processing it. This
  is a small optimization for streams with many short fields and comments; it also existed in
  the previous parser. Measure allocations for a long stream with fragmented chunks.
- **Avoid full request-body copies on each attempt.**
  [create_request](../lib/framework/src/http.rs#L511) clones the entire `String`, then transfers
  that allocation into `Bytes`. Store a replayable `Bytes` body once and clone its reference
  for each send, retaining text access for logging. This copy also existed before the transport
  rewrite, but is worth removing for large Elasticsearch bulk requests and retries. Check body
  equality across retries and measure allocations with a large payload.

## Validation and follow-up

- `git diff --check` passed.
- Attempted `cargo test -p framework --lib -p http_test -p framework_macro --locked --offline`.
  The sandbox could not access the Rust toolchain; a retry outside the sandbox failed because
  rustup has no default toolchain configured. No Rust tests or performance benchmarks ran.
  Findings are based on source inspection, with dependency behavior checked against the linked
  upstream sources; suggested reproductions above remain unexecuted.
- Existing h2c/concurrency tests use reqwest rather than the rewritten framework client.
  The new framework SSE round-trip uses its default HTTP/1 configuration. Add transport coverage
  for framework h2c, TLS/ALPN and custom roots, pooling and idle cleanup, stale pooled connections,
  headers/body deadlines and cancellation, and configured retries (including 503).
- Redirects, proxy environment variables, decompression, encoded-URI requirements, and the
  public API changes are already documented; this review treats those as deliberate decisions.
- When implementing fixes, update `/spec` for resulting behavior and design decisions, and
  `spec/test/http.md` for new integration coverage. This review alone changes no implementation
  contract, so no spec edits are included.
