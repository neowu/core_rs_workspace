# HTTP header logging tuning

Status: planned; no batching implemented.

Code: [`web/server.rs`](../lib/framework/src/web/server.rs),
[`log.rs`](../lib/framework/src/log.rs), [`log/action.rs`](../lib/framework/src/log/action.rs).
Contracts: [`action_log.md`](../spec/action_log.md), [`log_mask.md`](../spec/log_mask.md).

## Opportunity

Request headers, parsed cookies, and response headers currently emit one `log!` call per line.
Each call accesses and mutably borrows the task-local action and reads the clock for its elapsed
prefix. These costs are paid even when a successful action's trace is ultimately discarded.

## Proposed work

1. Add a small internal logging operation that borrows the action once for a synchronous block of
   header/cookie lines. Keep cookie parsing in the request header loop and write directly into the
   existing bounded log buffer; do not allocate a temporary collection or formatted block.
2. Initially preserve a timestamp per line. Separately measure sharing one elapsed timestamp per
   block; adopt that only if the saving justifies losing timing differences within a header block.
3. Apply batching to request and response headers. Preserve the existing `[header] name={value:?}`
   and `[cookie] name={value:?}` formats, percent-decoding, duplicate cookie entries, invalid-value
   handling, truncation limits, and deferred masking.
4. Keep the action borrow synchronous and scoped. Do not hold it across an await or call logging
   macros while it is held; recursive logging would panic on the task-local `RefCell` borrow.

## Verification and decision

- Check trace output and masking for ordinary headers, repeated and encoded cookies, invalid
  values, and truncation. Preserve no-op behavior outside an action.
- Use the real-socket [HTTP benchmark](../spec/benchmark/http_server.md): alternate baseline and
  candidate runs with identical settings, including representative requests with many headers and
  cookies. Compare server CPU per request and allocation counts; report throughput and latency too.
- Use multiple connections when measuring the server ceiling, and include both GET and POST.
- Profile before and after to verify that task-local access and timestamp work actually decrease.
  Keep the change only if repeated measurements show a benefit beyond run-to-run noise that
  warrants the extra logging code. No speedup is assumed in advance.
