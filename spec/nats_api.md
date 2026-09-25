# NATS api design

Code: [`framework_macro/src/nats_api.rs`](../lib/framework_macro/src/nats_api.rs),
[`framework_nats/src/service.rs`](../lib/framework_nats/src/service.rs) · siblings:
[`nats.md`](nats.md), [`http_server_api.md`](http_server_api.md),
[`benchmark/nats_api_server.md`](benchmark/nats_api_server.md)

## Behaviour

`#[nats_api]` on a trait generates both sides of one NATS request/reply contract:

- server: a provided `service(nats_client, Arc<Self>, ServiceConfig) -> Service` that registers
  every method on its subject via `Service::__add_handler`.
- client: `{Trait}Client` wrapping `ServiceClient`, implementing the same trait via `request`.

Method rules, enforced at expansion time:

- must be `async fn(&self [, request: Req]) -> Result<Res, Exception>`, at most one request param.
- `#[subject = "..."]` (or `#[subject("...")]`) is required.
- `service` is reserved.

`async fn` is rewritten to `fn -> impl Future<Output = ...> + Send`, same as `#[api]`.

A method without a request param is registered as a `()` handler, and the client sends `&()`, so
both sides share one `Service` / `ServiceClient` code path.

## Design decisions

### `fn` context name is a leaked `&'static str`, built once per handler

Same constraint as [`http_server_api.md`](http_server_api.md): the name depends on
`type_name::<Self>()`, known only at monomorphization, so `service()` formats it once per handler.

It's leaked to `&'static str` instead of captured as `String` and cloned per request. The request
cost is the same either way: one exact-size allocation, because `ContextValues` stores `String`.
The leak keeps the generated code the same as `#[api]` and drops the per-request clone. The leak is
bounded: one small string per handler per `service()` call, normally once at startup.

### handler is `Fn(Req) -> Fut`, not an async closure

`__add_handler` stores the handler in an `Arc` and needs a `'static` future, so the generated closure
clones the service `Arc` into each future. The handler itself is never cloned per request, unlike
axum handlers.
