# framework_http, hyper based HTTP server

Code: [`lib/framework_http`](../lib/framework_http/src) · siblings:
[`http_server.md`](http_server.md) (axum based server it replaces), [`action_log.md`](action_log.md),
[`test/framework_http.md`](test/framework_http.md)

Built directly on `hyper` + `hyper-util`, without axum / tower. It keeps the behaviour of the axum based
server ([`http_server.md`](http_server.md)): config, `/health-check`, the `http` action log, client
info, `active_http_requests` metrics, shutdown delay and graceful drain without deadline, `start_with_listener`.

## Protocol

- HTTP/1.1 and h2c share one port: `hyper_util::server::conn::auto` reads the connection preface and
  serves h2 when the client sends the h2 preface (prior knowledge), otherwise HTTP/1.1.
- `Upgrade: h2c` is not supported (deprecated by RFC 9113), such a client stays on HTTP/1.1.
- No TLS, it runs behind a load balancer.
- Both protocols get a `TokioTimer`, enabling hyper's default 30s HTTP/1.1 header read timeout.

## Routing

- Static paths only: exact match on `HashMap<&'static str, Vec<(Method, Route)>>`, then prefix routes
  (longest first) for directories. No path parameters, no extractors.
- Unknown path `404`, known path with other method `405` with `Allow`, both empty bodies, still logged as
  `http` actions without `matched_path` / `fn`.
- HEAD falls back to the GET handler, as axum did. The server drops the body of HEAD responses and keeps
  `content-length`, because hyper's h2 server still sends it and h2 clients reset the stream.
- Duplicate routes panic at registration (fail fast on startup).

## Controller

`async fn(state: Arc<S>, request: Request) -> Result<Response, Exception>`

- One fixed signature instead of axum's dynamic extractors: easier to read, no per-request handler
  clone. `Result` allows `?` on request parsing.
- `Router<S>` owns one `Arc<S>`, handlers are type erased `Box<dyn Fn(Request) -> Pin<Box<Future>>>`
  capturing a state clone, so routers with different state can be `merge`d into one server.
- `Err` is logged with the action and mapped to `ErrorResponse` JSON, same status mapping as `HttpError`
  (`BAD_REQUEST` / `VALIDATION_ERROR` 400, `NOT_FOUND` 404, `FORBIDDEN` 403, else 500).
- A panic in a controller is caught, logged as `handler panicked` error and returned as 500, so the
  action is still logged and an HTTP/1.1 connection is not dropped.
- `fn` context is `type_name_of_val(&handler)`, a `&'static str` taken at registration.

## Request / Response

- `Request` wraps `http::request::Parts` and the hyper body, exposing shortcuts: `path`, `query_string`,
  `header`, `cookie` (percent decoded, across multiple cookie headers as h2 splits them), `client_ip`,
  `user_agent`, `query::<T>()`, `body()`, `text()`, `json::<T>()`.
- Body is read once, bounded by `max_body_size` (default 2MB, same as axum's default limit); exceeding it
  or invalid input is a `BAD_REQUEST` warning. `text` / `json` / `query` log the input as axum extractors did.
- `Response` constructors: `empty()` (204), `json(&T)` (logs body and `response_content_length`), `text`,
  `html`, `bytes(body, content_type)`, with `status(..)` / `header(..)` builders.
- The response body is an own enum (`Empty` / `Full(Bytes)` / `File`) implementing `http_body::Body`,
  not `BoxBody`, avoiding an allocation and dynamic dispatch per response; sizes are exact so hyper sets
  `content-length`.

## Static files

`Router::dir(prefix, root)` and `Router::file(path, file)`, GET / HEAD only.

- Only normal path components are accepted after percent decoding, `..`, `.`, absolute paths and
  invalid utf-8 are `404`; a path ending with `/` serves `index.html`. Symlinks are followed.
- Open, stat and read run in one `spawn_blocking` call, files up to 1MB are read into memory in that call,
  larger files are streamed in 64KB chunks.
- `Last-Modified` / `If-Modified-Since` give `304`. No range requests, no ETag, no precompressed variants,
  content type from a small extension table.
- Missing files are `NOT_FOUND` warnings with the request path, never the file system path.
