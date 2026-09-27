# Log mask design

Code: [`log/mask.rs`](../lib/framework/src/log/mask.rs) · callers:
[`web/server.rs`](../lib/framework/src/web/server.rs), [`http.rs`](../lib/framework/src/http.rs) · siblings:
[`action_log.md`](action_log.md)

## Sensitive request values are masked by an exact-name list

`LogValue::new(key, value)` formats only the value, or `**masked**` when the key equals one of a
hard-coded list in `log/mask.rs` (currently only `authorization`); names are added as the need comes
up. Header names are always lowercase, so the match is plain equality.

It implements only `Debug`: `HeaderValue` has no `Display`, and one format keeps header and cookie
values quoted alike. Call sites keep the readable `key={:?}` form.

The check runs for every header of every request, so it stays an exact match over a short list
rather than a substring scan (the substring scan showed up at ~1.5% cpu in the http benchmark).

## http server

- request headers go through `LogValue`; the `cookie` header is not logged as a whole, each parsed
  cookie is logged instead, as-is: the server has no session support, so no cookie is expected to
  carry a secret; add cookie masking when session support is added
- response headers are logged as-is: they are set by app code, and without session support nothing
  sensitive (e.g. a session `set-cookie`) is put there; add `set-cookie` masking when sessions are added

## http client

- request headers go through `LogValue`
- response headers are logged as-is: the client has no cookie feature or cookie store, so
  `set-cookie` is not expected to matter; add `set-cookie` masking when cookie support is enabled
