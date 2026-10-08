# framework_http e2e test

Run `cargo test -p framework_http_test`, no external service needed. Code:
[`test/framework_http_test`](../../test/framework_http_test), design: [`framework_http.md`](../framework_http.md).

- Bind loopback port `0` and pass the listener to `HttpServer::start_with_listener`, wait for
  `/health-check`, shutdown after each test. `max_body_size` is 1KB to exercise the limit.
- Use reqwest with `http1_only` and `http2_prior_knowledge` clients to cover both protocols on one port.
- `request_test`: query / JSON / text handling, cookie, user agent and `x-forwarded-for` accessors,
  `merge` of routers with different state, `204`, HEAD fallback with `content-length`, malformed input and
  body limit (`400` / `BAD_REQUEST`), exceptions as `ErrorResponse`, panics as `500`, `404`, `405` with
  `Allow`, requests fail after shutdown.
- `h2c_test`: responses are `HTTP/2`, the request sees `HTTP/2.0`, 50 concurrent streams on one connection.
- `file_test`: content type, `index.html`, `304` with `If-Modified-Since`, a streamed 3MB file, HEAD
  without body, path traversal and directories are `404`, non GET `405`, over both protocols.
- Consume or drop every response body before shutdown: graceful drain has no deadline, an unread h2
  stream keeps the server waiting on flow control.
