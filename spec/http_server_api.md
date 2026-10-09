# HTTP server api design

Code: [`framework_macro/src/api.rs`](../lib/framework_macro/src/api.rs),
[`framework/src/web/api.rs`](../lib/framework/src/web/api.rs) · siblings:
[`http_server.md`](http_server.md), [`benchmark/http_server.md`](benchmark/http_server.md), [`nats_api.md`](nats_api.md)

## Behaviour

`#[api]` on a trait generates both sides of one HTTP contract from the trait definition:

- server: a provided `route(Arc<Self>) -> Router` that registers every method on its
  `#[path]` with its HTTP method, the service is the bound state, merge it into the server router.
- client: `{Trait}Client` wrapping `ApiClient`, implementing the same trait over `HttpClient`.

Method rules, enforced at expansion time:

- must be `async fn(&self [, request: Req]) -> Result<Res, Exception>`, at most one request param.
- `Req` must implement `Validator`, the route validates it before calling the method, see
  [`validate.md`](validate.md#api-request).
- exactly one of `#[get]` / `#[post]` / `#[put]`, plus `#[path("...")]`.
- `route` is reserved.

`async fn` is rewritten to `fn -> impl Future<Output = ...> + Send`, so impls can be spawned on the
multi-threaded runtime without the caller boxing.

Wire format:

- `GET` sends the request as query string (`request.query()` / `serde_html_form`), `POST` / `PUT` as JSON body
  (`request.json()`).
- `Ok(())` maps to `204 No Content`, other `Ok` to JSON, `Err` is returned to the server, which logs it and
  maps it to `ErrorResponse`, see [`http_server.md`](http_server.md#controller).
- the client sends `client` (app name) and `ref_id` (current action id) headers to link action logs;
  a non-2xx JSON `ErrorResponse` is rebuilt into an `Exception` keeping `severity` and `code`.

## Design decisions

### `fn` context name is built once per route, not per request

The `fn` context is `"{type_name::<Self>()}::{method}"`. The name depends on the implementing type,
which is only known at monomorphization, and `type_name` is not `const` on stable, so it can't be a
compile-time literal. The handler is a closure, its own type name is meaningless.

`route()` formats it once, leaks it to `&'static str` and registers it with the route through the
hidden `StateRouter::__route(method, path, name, handler)`; the server sets `context!(fn = ..)` like for
any other route:

- formatting per request cost a `format!` plus a realloc (empty first piece → zero initial
  capacity).
- the leak is bounded — one small string per route per `route()` call, normally once at startup.
- the handler closure captures nothing, the service comes in as the bound state `Arc<Self>`.

One allocation per request remains because `ContextValues` stores `String`; removing it means
changing `ContextValues` to `Cow<'static, str>`, not a macro change.

The client side emits `log!("call http api, fn={module_path}::{Trait}Client::{method}")` instead of `context!`: one
action can make many api calls, so per-call names would pollute the caller's action context. The
client type is generated, so the name is a compile-time `concat!(module_path!(), ...)` literal.

### `ApiClient` methods are macro-only

`__new` / `__get` / `__post` / `__put` are `#[doc(hidden)]` and `__`-prefixed: they only exist to back the
generated `{Trait}Client`, so they stay out of auto-complete and aren't a public calling convention.
