# HTTP e2e test

Run `cargo test -p http_test`, no external service needed. Code: [`test/http_test`](../../test/http_test),
design: [`http_server.md`](../http_server.md), [`http_server_api.md`](../http_server_api.md).

- Bind loopback port `0` and pass the listener to `HttpServer::start_with_listener`, wait for
  `/health-check`, shutdown after each test. `max_body_size` is 1KB to exercise the limit.
- Use only the framework `HttpClient` (no reqwest): `http_client(h2c)` builds a new client per use, HTTP/1
  or h2c (`prefer_http2`), to cover both protocols on one port and back the `#[api]` clients; readability
  over reuse. A missing client feature is added to the
  framework, not worked around with another client. The protocol is asserted server side (`h2c_test`).
- `sse_test` uses `HttpClient::sse` for streams (disconnect by dropping the `EventSource`, shutdown frame
  via `EventSource::retry()`) and `execute` for whole responses.
- `request_test`: query / JSON / text handling, Unicode and query escaping, cookie, user agent and
  `x-forwarded-for` accessors, duplicate or invalid `x-forwarded-for` and NUL in JSON body are `400`, a non-text `client` header still reaches the handler, `merge` of routers with
  different state, `204`, HEAD fallback with `content-length`, malformed input and body limit
  (`400` / `BAD_REQUEST`), exceptions as `ErrorResponse`, panics as
  `500`, `204` with a body set sends none over both protocols, `404`, `405` without `Allow`, requests fail
  after shutdown.
- `h2c_test`: responses are `HTTP/2`, the request sees `HTTP/2.0`, 50 concurrent streams on one connection.
- `file_test`: content type, `index.html`, conditional request headers ignored, empty file, a streamed 3MB
  file, HEAD without body, path traversal, dot (hidden files) / empty segments and directories are `404`,
  non GET `405`, over both protocols.
- `api_test`: `#[api]` generated routes and clients for GET, POST and PUT, no-argument calls, empty `204`
  responses, validation errors and service exceptions; status, JSON error bodies, and severity, code and
  message kept in client exceptions, including a non-JSON `404`; calls fail after shutdown.
- `api_interceptor_test`: an interceptor chained on `#[api]` `route` rejects with `403` and passes data to the
  service method through a scoped task local.
- `sse_test`: event encoding, headers and `last-event-id`, the stream ends when the closure returns, the
  controller rejecting with `403`, an error in the closure ends the stream normally, HEAD doesn't run the closure,
  client disconnect drops the closure, shutdown with open streams (one without any event) completes, each stream
  ends with a `retry` frame and its closure is dropped, over both protocols. Keepalive (15s) and a stalled h2
  client are not covered; the unit test covers the closure dropped on shutdown without the body being polled.
- `interceptor_test`: interceptors run in order around the handler of exact and prefix routes registered before or after them, short circuit
  with `403`, pass a request extension, don't apply to another router with the same state.
- `diagnostic_test`: sys routes on their own server, next to the app server; `/_sys/action` lists a request
  waiting in its app handler as `suspended` with its masked trace, not the dump request itself, drops it
  once it completes; `/_sys/thread` has the thread serving the request with its action id; both `404` on
  the app server. A `/_sys/` route on a non loopback listener panicking at start is a unit test.
- Consume or drop every response body before shutdown: graceful drain has no deadline, an unread h2
  stream keeps the server waiting on flow control.

The example contract uses `GreetRequest { name }` and `GreetResponse { greeting }`.
