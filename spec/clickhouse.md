# ClickHouse client design

Code: [`lib/framework_clickhouse/src/lib.rs`](../lib/framework_clickhouse/src/lib.rs),
[`types/datetime.rs`](../lib/framework_clickhouse/src/types/datetime.rs),
[`types/date.rs`](../lib/framework_clickhouse/src/types/date.rs),
[`types/decimal.rs`](../lib/framework_clickhouse/src/types/decimal.rs),
[`types/map.rs`](../lib/framework_clickhouse/src/types/map.rs),
[`framework_macro/src/enum8.rs`](../lib/framework_macro/src/enum8.rs) · consumers:
[`log_processor_rs`](../app/log_processor_rs/src/nats/action_handler.rs),
[`log_processor`](../app/log_processor/src/kafka) · tests:
[`test/clickhouse_test`](../test/clickhouse_test/tests) · sibling:
[`action_log.md`](action_log.md)

A thin wrapper over the `clickhouse` crate: a client, four statements (`execute`, `select_one`,
`select_all`, `insert`/`insert_borrowed`), and the column types the crate does not carry itself.
It adds nothing to the protocol — what it adds is an action-log span and write/read stats on every
statement, and a set of newtypes that make a rust value serialize correctly on **both** wires the
crate uses.

## One value, two wire formats

The crate serializes through serde twice, for different purposes, and the two need different bytes
for the same value:

| path | `is_human_readable` | carries a `DateTime` as |
|---|---|---|
| row data (`RowBinary`) | false | i64 milliseconds |
| query param (SQL) | true | `'2026-07-15T12:30:45Z'`, parsed by the server |

`DateTime64`, `Date16` and `Decimal64<S>` are newtypes that branch on `is_human_readable` inside
their own `Serialize`/`Deserialize`. The alternative — `#[serde(with = ..)]` on every field — puts
the decision at each call site, where it is invisible and easy to get wrong, and it cannot be
applied to a query param at all.

The branch is not symmetric, and neither side is optional:

- `Date16` binds as `'YYYY-MM-DD'`, because the server refuses to compare `Date` with a number.
- `DateTime64` binds as RFC3339 for the same reason: the millis form compares as a plain number
  against `DateTime64` and silently matches **nothing** — no error, no rows.

`Option<T>` works as-is for a `Nullable` column, so there is no `::option` helper variant.

### Range is checked on the way out, not clamped

`Date` is u16 days from 1970-01-01, so it ends at 2149-06-06. The server **clamps** anything past
that silently, so `Date16::serialize` fails instead. A row that cannot be represented is an error,
not a row with a different date in it.

### `Decimal64<S>` is integer math

`RowBinary` carries `Decimal64(S)` as the raw i64 scaled by `10^S`, which is exactly what
`#[serde(transparent)]` over i64 gives. Conversion to and from f64 exists for callers, but the
stored value and its `Debug` form are computed on integers, so they stay exact past the 15–16
significant digits f64 keeps. `S > 18` fails at compile time through const eval overflow.

### `Enum8` derives serde, not a clickhouse trait

A clickhouse `Enum8` column is an `Int8` on the wire. `#[derive(Enum8)]` generates
`Serialize`/`Deserialize` over the variant's explicit discriminant, and rejects a discriminant that
does not fit in i8 or a variant carrying fields. Deserialization of an unknown value is an error
rather than a fallback variant — a value the schema does not have means the rust enum and the table
have diverged, and a silent default would hide it.

The derive lives in `framework_macro`, because a proc macro crate cannot use its own derive; its
serde behaviour is covered by a test in `framework_clickhouse` instead.

## Params are bound, not formatted

`execute`/`select_one`/`select_all` take `&[&dyn QueryParam]` and each `?` in the sql is replaced by
the corresponding param, in order (`??` is a literal `?`). The crate's own `Bind` trait is sealed
and not object-safe, so params cannot be `&[&dyn Bind]` the way `framework_db` uses `&[&dyn ToSql]`;
`QueryParam` is a wrapper trait, blanket-implemented for `Serialize + Debug + Sync`, that folds each
param into `query.bind()`.

`Debug` is part of the bound because the sql and its params are written to the action log on every
statement. That is also why `Date16`/`DateTime64`/`Decimal64` implement `Debug` by hand: the log
should show `2026-07-15`, not the nested debug of the inner type, and not a raw scaled integer.

## Rows: owned and borrowed

`clickhouse::Row` distinguishes a row that owns its data (`RowOwned`) from one that borrows
(`Row` with a `Value<'a>` GAT). Both are supported, as two methods:

| method | rows | row type |
|---|---|---|
| `insert` | `&[T]`, `T: RowOwned` | inferred from the slice |
| `insert_borrowed` | `&[T::Value<'_>]` | named: `insert_borrowed::<ActionRow>(..)` |

A borrowed row reaches the crate as `T::Value<'_>` — the same `T` with its data lifetime pinned to
the slice — which no longer determines `T`, hence the turbofish. Keeping them separate means the
ordinary case, a row built and owned by the caller, stays inferred; only a row that points into
something else pays the annotation.

They are one implementation: `RowOwned` is defined as `for<'a> Row<Value<'a> = Self>`, so an owned
slice already *is* the borrowed form and `insert` is a call to `insert_borrowed`.

A single inferred method covering both is not expressible today. `T: Row<Value<'a> = T> + RowWrite`
fails to unify against `RowWrite`'s higher-ranked supertrait, and spelling that bound out as
`for<'x> T::Value<'x>: Serialize` fails differently, since the `for<'x>` cannot be constrained to
outlive `'a`.

### A borrowed row is how a consumer avoids copying a message it already has

The consumers here turn a batch of messages into a batch of rows and write it immediately. An owned
row forces a copy of every string out of the message, for a row that dies at the end of the call.
[`log_processor`](../app/log_processor/src/kafka) is the clear case: it builds a clickhouse row
*and* an elasticsearch document from the same message. Both now borrow from the original batch;
see [log processor](log_processor.md).

### `Map` columns serialize from an ordered slice

`types::Map<'a, K, V>` wraps `&'a [(K, V)]` and writes it as a `Map` column. A message that already
carries its key/value pairs as an ordered `Vec` — which
[the action record](action_log.md) does, deliberately — therefore needs no `HashMap` built per row:
nothing is allocated and no key is hashed on the way in.

Two properties follow from `Map` being an array of tuples in clickhouse, and both are relied on:

- **Order survives.** The insertion order a record kept end to end reaches the column.
- **A key may repeat.** Clickhouse accepts a duplicate key and resolves `map['key']` to the first.
  A `HashMap` row would have kept only the last, silently dropping a value the producer meant to
  record.

Log consumers rely on the [reserved framework-key convention](action_log.md#framework-keys-are-reserved-by-convention):
application context and stats must not collide with framework-generated keys. They intentionally
omit collision handling and overwrite-precedence guarantees to keep serialization simple and avoid
extra allocations and hashing.

## Behaviour

- **`insert` does not use `async_insert`.** It was set with `wait_for_async_insert=0` and produced
  **silent data loss** — no error on either side, nothing in `system.asynchronous_insert_log`. The
  insert now waits for the server, and a message handler waits with it.
- **Every statement opens a `clickhouse` span** and records `clickhouse_write_rows` /
  `clickhouse_write_bytes` on a write, `clickhouse_read_rows` on a read, so a slow or oversized
  query is visible in the action that issued it without extra instrumentation.
- **One batch is one insert.** `Inserter` is created per call and ended in the same call; there is
  no cross-call buffering that could outlive the caller.

## Known gaps

- **No schema check at startup.** A rust row whose columns drifted from the table fails at the first
  insert, at runtime, per app.
- **`Map` is write-side only.** Reading a `Map` column back deserializes into a `HashMap`, so the
  order the column preserves is dropped on the way out.
- **No `Decimal64` arithmetic.** It converts and compares; anything else goes through f64 and gives
  up the exactness the storage has.
