# HTTP server

Code: [`web/server.rs`](../lib/framework/src/web/server.rs) · siblings:
[`http_server_api.md`](http_server_api.md), [`action_log.md`](action_log.md),
[`metrics.md`](metrics.md), [`test/http.md`](test/http.md),
[`framework_http.md`](framework_http.md) (hyper based replacement)

## Startup

- `start` binds the configured address and serves the router.
- `start_with_listener` takes ownership of an already-bound Tokio `TcpListener`; the configured
  bind address is ignored. Callers can bind port `0`, read the assigned address, and pass the same
  listener without releasing the port.
- Startup logging reports the actual bound address, including the assigned port.

## Shutdown

- Cancellation waits for `shutdown_delay` (default zero) before stopping acceptance and starting
  graceful drain. This is a delay for load-balancer propagation, not a drain timeout.
- Requests continue to be served during the delay, and `/health-check` continues to return `200`.
  Load-balancer removal must be initiated externally; this endpoint does not signal readiness changes.
- After the delay, the server stops accepting new connections and gracefully drains existing ones.
  There is no drain deadline; an unfinished handler or response stream can prevent shutdown completing.
- `shutdown_delay` replaces the misleading `shutdown_grace_period` configuration field.

## Request lifetime

- The HTTP action and active-request counter cover middleware and handler execution until a response
  is returned, including request body reads performed by the handler or its extractors.
- Response body transmission, file downloads, SSE streams, and work detached from the handler are
  outside that lifetime. Streaming failures are not captured by the completed HTTP action.
- `active_http_requests` is the peak concurrent handler count since the previous collection,
  excluding `/health-check`; it is not a count of open connections or active response streams.
- HTTP action `elapsed` measures response construction, not completion of delivery to the client.
