# HTTP e2e test

Run `cargo test -p http_test`, no external service needed. Code: [`test/http_test`](../../test/http_test).

- Start the framework `HttpServer` on a dynamically selected loopback port and send
  real HTTP requests with the framework `HttpClient`.
- Wait for `/health-check` readiness with a deadline; bound requests and shutdown.
  Stop the server after each test, including cleanup on failure.
- Cover query, JSON, and text request/response handling, Unicode and query escaping,
  malformed input (`400` / `BAD_REQUEST`), unknown routes (`404`), and unsupported
  methods (`405`).
- Exercise `#[api]` generated routes and clients for GET, POST, and PUT, no-argument
  calls, empty `204` responses, validation errors, and service exceptions. Verify
  status, JSON error bodies, and preservation of severity, code, and message in
  client exceptions, including fallback handling for a non-JSON `404`.
- Verify requests fail after graceful server shutdown.

The example contract uses `GreetRequest { name }` and `GreetResponse { greeting }`.
See the [raw HTTP test](../../test/http_test/tests/http_test.rs) and
[API test](../../test/http_test/tests/api_test.rs) for definitions and assertions.

| Example request | Response |
| --- | --- |
| `GET /greet?name=world` | `200`, `{"greeting":"hello, world"}` |
| `POST /greet` with `{"name":"world"}` | `200`, `{"greeting":"hello, world"}` |
| `POST /echo` with text `hello` | `200`, text `hello` |
| `GET /api/greet?name=world` | `200`, `{"greeting":"hello, world"}` |
| `POST /api/greet` with `{"name":"world"}` | `200`, `{"greeting":"created, world"}` |
| `PUT /api/greet` with `{"name":"world"}` | `200`, `{"greeting":"updated, world"}` |
| `GET /api/ping` | `204`, empty body |
| `PUT /api/greet` with `{"name":""}` | `400`, error code `VALIDATION_ERROR` |
| `POST /api/fail` | `500`, error code `TEST_FAILURE`, warning severity, message `expected failure` |

The HTTP API contract is specified in [http_server_api.md](../http_server_api.md).
