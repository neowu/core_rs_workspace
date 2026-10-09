# HTTP server

Code: [`web`](../lib/framework/src/web) · siblings:
[`http_server_api.md`](http_server_api.md), [`action_log.md`](action_log.md),
[`metrics.md`](metrics.md), [`test/http.md`](test/http.md)

Built directly on `hyper` + `hyper-util`, without axum / tower (replaced axum, which brought dynamic
extractors, per request handler clones and a tower middleware stack the framework didn't need).

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
- SSE streams are closed when acceptance stops, see [SSE](#sse); like any response, a client not reading
  (h2 flow control) can still hold the drain.

## Request lifetime

- The HTTP action and active-request counter cover routing and handler execution until a response
  is returned, including request body reads performed by the handler.
- Response body transmission and file downloads are outside that lifetime. Streaming failures are not
  captured by the completed HTTP action.
- `active_http_requests` is the peak concurrent handler count since the previous collection,
  excluding `/health-check`; it is not a count of open connections or active response streams.
- HTTP action `elapsed` measures response construction, not completion of delivery to the client.
- The per-request future stays small (~330 B): hyper spawns it per h2 stream and tokio moves it by value
  several times, so `handle` is a plain fn returning an `async move` block (an `async fn` holds its params
  twice) and boxes the `log::action` future (one malloc instead of memcpy of ~1.6 KB on every move).
  Profile: `report/2026-10-09_http_perf`.

## Protocol

- HTTP/1.1 and h2c share one port: `hyper_util::server::conn::auto` reads the connection preface and
  serves h2 when the client sends the h2 preface (prior knowledge), otherwise HTTP/1.1.
- `Upgrade: h2c` is not supported (deprecated by RFC 9113), such a client stays on HTTP/1.1.
- No TLS, it runs behind a load balancer.
- Both protocols get a `TokioTimer`, enabling hyper's default 30s HTTP/1.1 header read timeout.
- Protocol detection (reading the h2 preface) has no timeout: an idle or partial preface connection
  stays open. Expected deployment is behind an L7 load balancer (GCP HTTP(S) LB / ALB / GKE ingress),
  which terminates client connections and only forwards complete requests, so slow / idle clients
  never reach the server. Not safe behind an L4 passthrough LB or with the port exposed directly.
- `204` / `304` responses never send a body or `content-length` (hyper's h2 server would send them).

## Routing

- Static paths: exact match on `HashMap<&'static str, Vec<(Method, Route)>>`, then prefix routes
  (longest first). No path parameters, no extractors.
- `prefix(method, prefix, handler)` covers paths with a variable tail (e.g. `/event/{app}`),
  the handler reads the tail from `request.path()`; `dir` is a GET prefix route.
- Unknown path `404`, known path with other method `405` without `Allow` (mostly vulnerability scans, don't
  hint available methods), both empty bodies, still logged as `http` actions without `path` / `fn`.
- HEAD falls back to the GET handler. The server drops the body of HEAD responses and keeps
  `content-length`, because hyper's h2 server still sends it and h2 clients reset the stream.
- Duplicate routes panic at registration (fail fast on startup).

## Controller

`async fn(state: Arc<S>, request: Request) -> Result<Response, Exception>`

- One fixed signature instead of dynamic extractors: easier to read, no per-request handler
  clone. `Result` allows `?` on request parsing.
- `Router` has no state type: `Router::new().state(Arc<S>)` returns a `StateRouter<S>` carrying the routes so far,
  handlers registered on it (`.get(..)`) are bound to that state, `into()` converts back to `Router`. Plain
  builder calls instead of a closure, easier to read at the call site. Each handler is type erased into
  `Box<dyn Fn(Request) -> Pin<Box<Future>>>` capturing a state clone. Routers built with different states
  are `merge`d into one (`merge` takes `impl Into<Router>`), the server and module fns only deal with `Router`;
  `dir` / `file` need no state, call them before `state`.
- `Err` is logged with the action and mapped to `ErrorResponse` JSON
  (`BAD_REQUEST` / `VALIDATION_ERROR` 400, `NOT_FOUND` 404, `FORBIDDEN` 403, else 500).
- A panic in a controller is caught, logged as `handler panicked` error and returned as 500, so the
  action is still logged and an HTTP/1.1 connection is not dropped. The handler is invoked inside the
  caught future, covering handlers that panic before returning their future.
- `fn` context is `type_name_of_val(&handler)`, a `&'static str` taken at registration; `#[api]` passes
  its own name, see [`http_server_api.md`](http_server_api.md).

## Request / Response

- `Request` wraps `http::request::Parts` and the hyper body, exposing shortcuts: `path`, `query_string`,
  `header`, `cookie` (percent decoded, across multiple cookie headers as h2 splits them), `client_ip`
  (`x-forwarded-for` within `max_forwarded_ips` hops, else peer address), `user_agent`, `query::<T>()`,
  `body()`, `text()`, `json::<T>()`.
- Body is read once, bounded by `max_body_size` (default 2MB); exceeding it or invalid input is a
  `BAD_REQUEST` warning. `text` / `json` / `query` log the input.
- `Response` constructors: `empty()` (204), `json(&T)` (logs body and `response_content_length`), `text`,
  `html`, `bytes(body, content_type)`, with `status(..)` / `header(..)` builders.
- The response body is an own enum (`Empty` / `Full(Bytes)` / `File` / `Sse`) implementing `http_body::Body`,
  not `BoxBody`, avoiding an allocation and dynamic dispatch per response; sizes are exact so hyper sets
  `content-length`.

## SSE

A regular controller returns `Response::sse(|channel| async move { .. })`, the closure streams events.

- Two actions: the controller is a normal `http` action (`200`, `text/event-stream`, `cache-control: no-cache`,
  no `content-length`, chunked on HTTP/1.1), then the closure runs in a spawned task as the `sse` action with
  `ref_id` of the `http` action, the same `path` / `fn` context, its own logs, and `sse_write_entries` /
  `sse_write_bytes` (queued events); `elapsed` is the stream duration. Keeps `handle` / `process` and routing
  unchanged, simpler than one action that has to return headers to hyper and keep running.
- Rejecting a request (auth, validation) is the controller returning `Err` before `Response::sse`, a normal
  error response, which also stops `EventSource` reconnecting; `Response::empty()` (`204`) as well. Headers are
  sent right away, after that an `Err` / panic in the closure only ends the stream (logged with the `sse` action,
  the client reconnects).
- The closure starts after the `http` action, never for HEAD (the body is dropped). Its lifetime is the stream
  lifetime: the stream ends when it returns (after queued events, channel clones kept elsewhere don't hold it)
  or when every channel is dropped, and the closure future is dropped when the stream closes (client
  disconnected or shutdown), so cleanup belongs in `Drop`. Not counted in `active_http_requests`,
  `active_sse_streams` is the peak open stream count.
- SSE data is not critical by design convention: no delivery guarantee, no replay; on reconnect the controller
  reads `last-event-id` and the closure decides what to resend.
- Bounded buffer (16 events, counts events not bytes, limit payload size in the app): `send` waits for space,
  `try_send` fails when full (broadcast without waiting on a slow client). Both return `false` once the stream
  is closed. An event is encoded and logged as `[sse]` in the sender's action only once the buffer accepts it,
  accepted is not delivered.
- `Event::new(data)` writes one `data:` line per CR / LF / CRLF line, a trailing line break keeps a trailing
  empty `data:` line so the client gets the same text back, `Event::json`, `id` / `event` builders panic on
  line breaks (programmer supplied values).
- `:` comment keepalive after 15s without an event, below common LB idle timeouts. GCP HTTP(S) LB backend
  timeout caps the whole response duration, it has to be raised for long streams, clients reconnect.
- Shutdown: when acceptance stops, closures are dropped right away, every stream discards queued events, sends
  `retry: <1000..5000ms random>` to spread reconnects and ends. The last two need the transport to progress:
  best effort, a stalled client can hold the drain like any unread response (no drain deadline).

## Static files

`Router::dir(prefix, root)` and `Router::file(path, file)`, GET / HEAD only.

Local / dev use only, prod static files are served by CDN, so it stays minimal.

- The path is denied (`404`), not normalized, if after percent decoding any segment is empty or starts with
  `.` (`.`, `..`, hidden files like `.env` / `.git`), or it contains `\` / nul, or is invalid utf-8; a path
  ending with `/` serves `index.html`. Symlinks are followed. Only regular files are served.
- Open and stat run in one `spawn_blocking` call, the file is streamed in 64KB chunks, limited to the stat
  length so a growing file can't exceed `content-length`.
- No conditional requests (`Last-Modified` / `ETag` / `304`), no range requests, no precompressed variants,
  content type from a small extension table.
- Missing files are `NOT_FOUND` warnings with the request path, never the file system path.
