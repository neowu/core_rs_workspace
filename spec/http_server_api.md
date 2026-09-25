# HTTP server api design

Code: [`framework_macro/src/api.rs`](../lib/framework_macro/src/api.rs),
[`framework/src/web/api.rs`](../lib/framework/src/web/api.rs) · siblings:
[`benchmark/http_server.md`](benchmark/http_server.md)

## Behaviour

`#[api]` on a trait generates both sides of one HTTP contract from the trait definition:

- server: a provided `route(Arc<Self>) -> Router` that registers every method on its
  `#[path]` with its HTTP method.
- client: `{Trait}Client` wrapping `ApiClient`, implementing the same trait over `HttpClient`.

Method rules, enforced at expansion time:

- must be `async fn(&self [, request: Req]) -> Result<Res, Exception>`, at most one request param.
- exactly one of `#[get]` / `#[post]` / `#[put]`, plus `#[path("...")]`.
- `route` is reserved.

`async fn` is rewritten to `fn -> impl Future<Output = ...> + Send`, so impls can be spawned on the
multi-threaded runtime without the caller boxing.

Wire format:

- `GET` sends the request as query string (`Query` / `serde_html_form`), `POST` / `PUT` as JSON body.
- `Ok(())` maps to `204 No Content`, other `Ok` to JSON, `Err` goes through `HttpError`.
- the client sends `client` (app name) and `ref_id` (current action id) headers to link action logs;
  a non-2xx JSON `ErrorResponse` is rebuilt into an `Exception` keeping `severity` and `code`.

## Design decisions

### `fn` context name is built once per route, not per request

Each handler logs `context!(fn = "{type_name::<Self>()}::{method}")`. The name depends on the
implementing type, which is only known at monomorphization, and `type_name` is not `const` on
stable, so it can't be a compile-time literal.

`route()` formats it once and leaks it to `&'static str`, captured by the handler closure:

- formatting per request cost a `format!` plus a realloc (empty first piece → zero initial
  capacity).
- capturing a `String` is worse: axum clones the handler per request, so it would allocate twice.
- the leak is bounded — one small string per route per `route()` call, normally once at startup.

One allocation per request remains because `ContextValues` stores `String`; removing it means
changing `ContextValues` to `Cow<'static, str>`, not a macro change.
