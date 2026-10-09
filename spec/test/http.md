# HTTP e2e test

Run `cargo test -p http_test`, no external service needed. Code: [`test/http_test`](../../test/http_test),
design: [`http_server.md`](../http_server.md), [`http_server_api.md`](../http_server_api.md).

- Bind loopback port `0` and pass the listener to `HttpServer::start_with_listener`, wait for
  `/health-check`, shutdown after each test. `max_body_size` is 1KB to exercise the limit.
- Use reqwest with `http1_only` and `http2_prior_knowledge` clients to cover both protocols on one port,
  and the framework `HttpClient` for `#[api]` clients.
- `request_test`: query / JSON / text handling, Unicode and query escaping, cookie, user agent and
  `x-forwarded-for` accessors, a non-text `client` header still reaches the handler, `merge` of routers with
  different state, `204`, HEAD fallback with `content-length`, malformed input and body limit
  (`400` / `BAD_REQUEST`), exceptions as `ErrorResponse`, panics (in the future or before returning it) as
  `500`, `204` with a body set sends none over both protocols, `404`, `405` without `Allow`, requests fail
  after shutdown.
- `h2c_test`: responses are `HTTP/2`, the request sees `HTTP/2.0`, 50 concurrent streams on one connection.
- `file_test`: content type, `index.html`, conditional request headers ignored, empty file, a streamed 3MB
  file, HEAD without body, path traversal, dot (hidden files) / empty segments and directories are `404`,
  non GET `405`, over both protocols.
- `api_test`: `#[api]` generated routes and clients for GET, POST and PUT, no-argument calls, empty `204`
  responses, validation errors and service exceptions; status, JSON error bodies, and severity, code and
  message kept in client exceptions, including a non-JSON `404`; calls fail after shutdown.
- Consume or drop every response body before shutdown: graceful drain has no deadline, an unread h2
  stream keeps the server waiting on flow control.

The example contract uses `GreetRequest { name }` and `GreetResponse { greeting }`.
