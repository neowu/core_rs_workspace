# Log mask design

Code: [`log/mask.rs`](../lib/framework/src/log/mask.rs), called from
[`log/action.rs`](../lib/framework/src/log/action.rs) · log formats relied on:
[`web/server.rs`](../lib/framework/src/web/server.rs), [`http.rs`](../lib/framework/src/http.rs) · siblings:
[`action_log.md`](action_log.md)

## Sensitive values are masked when the trace is flushed

Everything is logged as-is, and when the trace is flushed (`Action::finish`, only when
`flush_trace()`), the whole trace is scanned once and the quoted value of every key in a hard-coded
name list (`MASKED_KEYS`, currently `authorization` and `password`) is replaced by `**masked**`.
Names are added as the need comes up.

- deferred to flush: the trace is emitted only on warn/error or `log::trace()`, so a successful
  action never pays for masking, and nothing is added to the request path
- the whole trace, not marked segments or known line prefixes: no call site opts in, and a changed
  log format or a new transport cannot silently stop masking; a mistaken match only masks too much.
  A marked scan saved time only for traces with little json, where the scan is already cheap
- one list for json fields, headers and cookies: a json `authorization` or a `password` cookie being
  masked is harmless
- `**masked**`, not the value overwritten with `*` in place: that keeps the value length secret, and
  in place saved nothing measurable, the scan dominates and the rebuild copy happens only when
  something was masked
- `error_message` is not masked: engineers must not put sensitive values into warn/error messages

## Two forms are matched, by a rough matcher rather than a parser

One pass over the `:` and `=` positions (`memchr2`), the list is compared only there:

- json field: `"name"`, optional whitespace, `:`, optional whitespace, a quoted value
- header / cookie: a whole word `name` (preceded by a space), `=`, a quoted value; header and cookie
  lines must be logged as `name={value:?}`, `Debug` of `&str` and `HeaderValue` quotes and escapes
  `"`. `proxy-authorization` is a different key and is not masked

The value runs to the unescaped closing quote. On well formed json and `Debug` output an escaped
quote cannot be part of a match, so text inside a string value is never taken as a key.

- clients must send well formed json; only string values are masked, a key written with escapes or
  a non string value is logged as-is, and a client that breaks the contract may get its data logged
- a value cut by the message limit is masked up to the line end
- a `HeaderValue` marked sensitive prints `Sensitive` unquoted, nothing to mask
- cost on 10 names: ~6µs for a 14KB trace, ~250µs for a 500KB one, below the json escaping the
  trace already costs when `NatsAppender` sends it

## http server

- request and response headers are logged as `[header] name={value:?}`; the `cookie` header is not
  logged as a whole, each parsed cookie is logged as `[cookie] name={value:?}`
- cookie names and values are percent-decoded; every valid pair is logged in header order,
  including duplicate names. Invalid cookie pairs and non-text cookie headers are skipped.
  Logging parses borrowed header slices without building an owned cookie jar.
- there is no session support yet; when it is added, mask the session cookie and `set-cookie` by
  adding their names to the list

## http client

- request and response headers are logged as `[header] name={value:?}`; the client has no cookie
  feature or cookie store
