# Reference — Lifetimes

Part of [Rust & Framework Training](00-outline.md) · Appendix reference, read alongside
[Session 1](01-session-1.md)

> **Reference, not a session.** Session 1 introduces lifetimes as "the names we give borrow
> durations". This document is the page you come back to when the compiler starts asking for
> `<'a>` and you are not sure what it wants. Every example is either runnable as written or points
> at real code in this workspace.

---

## 1. The one-paragraph model

A lifetime is a **region of code** during which a borrow is valid. It is not a duration in
milliseconds, not a GC generation, and not something that exists at runtime — every lifetime is
erased before codegen. Its only job is to let the compiler prove, at compile time, that no
reference outlives the value it points at. You never *create* a lifetime; a value's lifetime is
already determined by where it is owned. Annotations like `'a` only *describe relationships*
between lifetimes that already exist, in the places where the compiler cannot infer them on its
own — mainly function signatures and struct definitions.

The corollary is the single most useful rule for people coming from Java or TypeScript:

> **You cannot fix a lifetime error by adding lifetimes.** Annotations state a fact. If the fact
> is not true, the code still does not compile. The fix is to change who owns what — clone, take
> ownership, restructure — or to shorten the borrow.

**Coming from Java/TS:** the GC answers "is this reference still valid?" at runtime by keeping the
object alive. Rust answers it at compile time by refusing to compile a reference that could dangle.
Same question, different moment.

---

## 2. Syntax in one screen

| Form | Meaning |
|---|---|
| `&T` | shared borrow, lifetime inferred |
| `&'a T` | shared borrow valid for the region named `'a` |
| `&'a mut T` | exclusive borrow valid for `'a` |
| `fn f<'a>(x: &'a str) -> &'a str` | output borrows from `x` |
| `struct S<'a> { x: &'a str }` | `S` may not outlive whatever `x` points at |
| `S<'_>` | "there is a lifetime here, infer it" — the anonymous lifetime |
| `'static` | valid for the whole program |
| `T: 'a` | every borrow inside `T` outlives `'a` |
| `'a: 'b` | `'a` outlives `'b` (`'a` is at least as long as `'b`) |
| `for<'a> Fn(&'a str)` | works for *every* `'a` — a higher-ranked bound |
| `impl Trait + use<'a, T>` | the returned opaque type captures exactly `'a` and `T` |

---

## 3. Elision — why most code has no annotations

The compiler applies three rules to `fn` signatures before asking you for anything:

1. Each elided **input** lifetime gets its own fresh parameter.
2. If there is exactly **one** input lifetime, it is assigned to every elided **output** lifetime.
3. If one input is `&self` / `&mut self`, **`self`'s lifetime** is assigned to every elided output
   lifetime (rule 3 wins over rule 2).

Rule 3 is why the framework's most-used borrowing method needs no annotation at all:

```rust
// lib/framework/src/string.rs
pub trait StringExt {
    fn truncate_to_max(&self, len: usize) -> &str;
}
```

Two input lifetimes (`&self` and none from `usize`), one output — rule 3 ties the returned `&str`
to `&self`. Written out in full it is `fn truncate_to_max<'a>(&'a self, len: usize) -> &'a str`.
That signature is the entire contract: the returned slice points **into** the receiver, no
allocation happened, and the receiver must outlive the result.

Elision fails as soon as there are two candidate input lifetimes and an output reference. That is
exactly when you must write the annotation yourself — the next section.

---

## 4. Lifetimes on generic functions

### 4.1 The basic shape: "which argument does the result borrow from?"

This pattern appears once per transport crate in this workspace:

```rust
// lib/framework_nats/src/service.rs:284
fn header<'a>(message: &'a Message, name: &str) -> Option<&'a str> {
    message.headers.as_ref()?.get(name).map(|value| value.as_str())
}
```

Two references go in, one comes out. Elision rule 1 gives each input its own lifetime; rule 2 does
not apply (there are two), so the compiler cannot guess and reports **E0106: missing lifetime
specifier**. The annotation answers the only question it has: the result borrows from `message`,
never from `name`. That is not decoration — it is what lets a caller pass a temporary `name`:

```rust
let ref_id = header(&message, &format!("{prefix}-ref-id")); // temporary String, dropped here
log!("ref_id={ref_id:?}");                                  // still fine: ref_id borrows `message`
```

Had the signature said `-> Option<&str>` with both inputs sharing one lifetime, this would not
compile. The same shape shows up in `lib/framework_kafka/src/consumer.rs:353` and in
`app/log_processor_rs/src/alert.rs:188`:

```rust
fn context_value<'a>(context: &'a [(String, Vec<String>)], key: &str) -> Option<&'a str> {
    context.iter().find(|(name, _)| name.as_str() == key).and_then(|(_, values)| values.first()).map(String::as_str)
}
```

### 4.2 Lifetimes and type parameters together

`<'a, T>` mixes both kinds of generic parameter; lifetimes always come first in the list.

```rust
// lib/framework_db/src/field.rs:54
pub(crate) fn build_conditions<'a, T>(
    conditions: &'a [Cond<T>],
    sql: &mut String,
    params: &mut Vec<&'a QueryParam>,
    param_index: &mut i32,
) {
```

Read the signature as a data-flow statement: **`params` is filled with references that point into
`conditions`**, so `conditions` must outlive the `Vec` the caller passes in. `sql` and
`param_index` get their own elided lifetimes because nothing borrowed from them escapes. The caller
then hands `params` straight to tokio-postgres, and the compiler has already proved the parameter
slice cannot dangle while the query runs. Without the shared `'a`, the same code needs owned
`Box<dyn ToSql>` clones per query.

### 4.3 A lifetime as a *type*'s parameter: `T: Deserialize<'a>`

```rust
// lib/framework/src/json.rs:8
pub fn from_json<'a, T>(json: &'a str) -> Result<T, Exception>
where
    T: Deserialize<'a>,
{
    serde_json::from_str(json).map_err(|err| exception!(format!("failed to deserialize, json={json}"), source = err))
}
```

`Deserialize<'a>` is serde's *borrowing* deserialization trait: it says `T` may contain `&'a str`
slices pointing into the input buffer. The bound propagates the constraint outward — a `T` that
borrows forces `json` to outlive the returned value; a `T` that owns all its data (the common case,
every `#[derive(Deserialize)]` struct with `String` fields) implements `Deserialize<'de>` for *any*
`'de` and so places no constraint on the caller at all.

Contrast with the NATS service, which cannot borrow because the payload is dropped when the handler
future finishes:

```rust
// lib/framework_nats/src/service.rs:75
Req: DeserializeOwned + Send + 'static,
```

`DeserializeOwned` is the shorthand for `for<'de> Deserialize<'de>` — "does not borrow from the
input, at all". Rule of thumb in this workspace: **borrow when the buffer clearly outlives the
result (parsing a header, slicing a config file); own when the value crosses a task, a channel or
an `await` that outlives the buffer.**

---

## 5. The signature is the whole contract

Everything above is mechanics. This section is the *rule* those mechanics follow, and it is the one
idea that makes lifetime errors stop being arbitrary.

> **A lifetime parameter is a generic parameter, and it is checked exactly like `T`.** A function
> body is checked **once, generically**, against its own signature — never against any call site.
> A call site is checked against the **declared signature only** — never against the body.

The `T` analogy is worth taking literally. The body of `fn f<T: Display>(t: T)` may not assume
`T = String`, even if every caller in the workspace passes a `String`. In the same way, the body of
`fn f<'a>(x: &'a str)` may not assume `'a` is long, even if every caller passes a `&'static str`.
And the caller of `f` may not assume anything about what the body does with `t` beyond `Display`.

Two independent walls follow, and every surprising lifetime error is one of them. All compiler
output below is real, from `rustc --edition 2024`.

### 5.1 Wall 1 — the call site cannot see the body

The signature below ties `message` and `name` to one region. The body demonstrably never reads
`name`:

```rust
fn header_shared<'a>(message: &'a str, name: &'a str) -> Option<&'a str> {
    let _ = name;                            // body NEVER reads name
    message.split_once('=').map(|(_, v)| v)  // result borrows only `message`
}

fn main() {
    let message = String::from("ref-id=abc");
    let result;
    {
        let name = String::from("ref-id");
        result = header_shared(&message, &name);
    }
    println!("{result:?}");
}
```

```text
error[E0597]: `name` does not live long enough
11 |         result = header_shared(&message, &name);
   |                                          ^^^^^ borrowed value does not live long enough
12 |     }
   |     - `name` dropped here while still borrowed
13 |     println!("{result:?}");
   |                ------ borrow later used here
```

Change **only** the signature to `name: &str` — byte-identical body — and it prints `Some("abc")`.
This is why the workspace's `header` helpers are written with split lifetimes
(`lib/framework_nats/src/service.rs:284`, `lib/framework_kafka/src/consumer.rs:353`): the second
lifetime is not stylistic, it is the difference between a caller being able to pass a temporary
`name` or not.

The sharpest form of Wall 1 is a **single body under two signatures**:

```rust
struct Cache { hits: u64 }

impl Cache {
    // elision rule 3: the result is declared to borrow from &mut self
    fn label_elided(&mut self) -> &str {
        self.hits += 1;
        "<none>"                      // a genuine &'static str
    }

    // identical body, honest signature
    fn label_static(&mut self) -> &'static str {
        self.hits += 1;
        "<none>"
    }
}

fn main() {
    let mut cache = Cache { hits: 0 };

    let a = cache.label_static();
    cache.hits += 100;                  // fine: no live borrow
    println!("static: {a} hits={}", cache.hits);

    let b = cache.label_elided();
    cache.hits += 100;                  // <-- the only line that fails
    println!("elided: {b}");
}
```

Both return the *same literal*. The caller is free after `label_static()` and stuck after
`label_elided()`:

```text
error[E0503]: cannot use `cache.hits` because it was mutably borrowed
  --> c.rs:25:5
   |
24 |     let b = cache.label_elided();
   |             ----- `cache` is borrowed here
25 |     cache.hits += 100;                  // <-- the only line that fails
   |     ^^^^^^^^^^^^^^^^^ use of borrowed `cache`
26 |     println!("elided: {b}");
   |                        - borrow later used here
```

The borrow checker never inspects the returned expression. It reads "output tied to `&mut self`"
off the signature and assumes the worst. **This pair is the recommended live-coding beat**: type
one method, copy it, change only the return type, and let the group watch one call site break.

### 5.2 Wall 2 — the body cannot see the call sites

The dual. Every call here passes a literal, which really is `&'static str`:

```rust
struct Registry { keys: Vec<&'static str> }

impl Registry {
    fn add<'a>(&mut self, key: &'a str) {
        self.keys.push(key);
    }
}

fn main() {
    let mut r = Registry { keys: Vec::new() };
    r.add("subject");   // &'static str
    r.add("client");    // &'static str
}
```

The compiler's own wording is the clearest statement of the rule to be found anywhere:

```text
error[E0521]: borrowed data escapes outside of method
4 |     fn add<'a>(&mut self, key: &'a str) {
  |            --             --- `key` is a reference that is only valid in the method body
  |            |
  |            lifetime `'a` defined here
5 |         self.keys.push(key);
  |         ^^^^^^^^^^^^^^^^^^^ argument requires that `'a` must outlive `'static`
  |
  = note: requirement occurs because of a mutable reference to `Vec<&str>`
  = note: mutable references are invariant over their type parameter
```

*"`key` is a reference that is only valid in the method body."* Inside the body, `'a` is an opaque
region of unknown extent, regardless of what every caller actually does. The fix is signature-only:
`key: &'static str`. This is exactly why `context!`, `stats!`, `error_code`, `location` and `kind`
all take `&'static str` in `lib/framework/src/log.rs` instead of a generic `&'a str`, and why
`intern()` exists for the values that are not literals.

### 5.3 Implementing an *existing* signature

When the signature is handed to you — a trait method — the rule bites hardest, because the impl may
only be **at least as general as the declaration**. Three distinct outcomes, worth knowing apart:

**You may not narrow the inputs.**

```rust
trait Named { fn name(&self) -> &str; }
struct Borrowed<'a> { name: &'a str }

impl<'a> Named for Borrowed<'a> {
    fn name(&'a self) -> &'a str { self.name }   // demands self be borrowed for all of 'a
}
```

```text
error[E0308]: method not compatible with trait
  = note: expected signature `fn(&Borrowed<'_>) -> &_`
             found signature `fn(&'a Borrowed<'_>) -> &'a _`
note: the anonymous lifetime defined here... does not necessarily outlive the lifetime `'a`
```

The trait promised to work for *any* `&self` borrow; an impl that only works for one specific long
borrow is not a valid implementation of that promise.

**You may not widen the output beyond the data you hold.**

```rust
trait Named { fn kind(&self) -> &'static str; }

impl<'a> Named for Borrowed<'a> {
    fn kind(&self) -> &'static str { self.name }
}
```

```text
error: lifetime may not live long enough
4 | impl<'a> Named for Borrowed<'a> {
  |      -- lifetime `'a` defined here
5 |     fn kind(&self) -> &'static str { self.name }
  |                                      ^^^^^^^^^ returning this value requires that `'a` must outlive `'static`
```

**You may be more precise in the return position — and it buys the caller nothing.** This one
compiles, because return position is covariant and `'a: '_` holds by well-formedness of
`&'_ Borrowed<'a>`:

```rust
impl<'a> Named for Borrowed<'a> {
    fn name(&'_ self) -> &'a str { self.name }   // truthfully returns the LONG borrow
}
```

And it is useless, because the caller is checked against the trait's signature. The whole program,
so the line numbers below resolve:

```rust
trait Named {
    fn name(&self) -> &str;              // trait says: the result borrows from &self
}

struct Borrowed<'a> { name: &'a str }

impl<'a> Named for Borrowed<'a> {
    fn name(&'_ self) -> &'a str { self.name }   // impl truthfully returns the LONG borrow
}

fn main() {
    let owner = String::from("alert");           // outlives everything below
    let via_trait;
    let via_inherent;
    {
        let b = Borrowed { name: &owner };
        via_inherent = b.name;                   // field access: really &'a str, fine
        via_trait = Named::name(&b);             // through the declared signature
    }                                            // `b` dropped here
    println!("{via_inherent} {via_trait}");
}
```

```text
error[E0597]: `b` does not live long enough
  --> t.rs:18:33
   |
16 |         let b = Borrowed { name: &owner };
   |             - binding `b` declared here
17 |         via_inherent = b.name;                   // field access: really &'a str, fine
18 |         via_trait = Named::name(&b);             // through the declared signature
   |                                 ^^ borrowed value does not live long enough
19 |     }                                            // `b` dropped here
   |     - `b` dropped here while still borrowed
20 |     println!("{via_inherent} {via_trait}");
   |                               --------- borrow later used here
```

The impl body genuinely returns a borrow of `owner`, which lives to the end of `main`. The caller
gets a borrow of `b`, because that is what the trait declared. Extra precision inside an impl is
discarded at the boundary — so put the precision in the *signature* or not at all. See §10 for the
same effect from the other side: `Timestamp` copies out of the raw buffer and needs no relation to
`'a` whatsoever, but `FromSql<'a>` fixed the signature, so the impl carries `'a` regardless.

### 5.4 What follows from this

- **Monomorphization and inlining change nothing.** Each body is borrow-checked once, generically,
  before codegen. There is no per-call-site re-check, ever — not for a generic function, not for a
  `#[inline]` one, not for a private helper with exactly one caller.
- **Lifetime relations are API surface.** Merging two lifetime parameters into one is a breaking
  change with an untouched body; adding `where 'a: 'b` or `+ use<>` repairs callers with an
  untouched body. Edition 2024's `impl Trait` capture change (§9) broke callers by changing a
  default *in the signature* and nothing else.
- **Closures are the exception that proves the rule.** A closure's parameter lifetimes are inferred
  from its use rather than declared, which is precisely why a closure bound has to be written
  higher-ranked (§8) to be expressible at all.
- **Therefore: design the signature deliberately.** For anything with more than one reference
  parameter, decide which output borrows from which input and write it down, even where elision
  would have picked something that happens to compile today. The signature is the only thing your
  callers will ever see.

---

## 6. `'a: 'b` — outlives bounds

`'a: 'b` reads "**`'a` outlives `'b`**", i.e. the region `'a` is at least as long as `'b`. Think
of it as `'a ⊇ 'b`, not as a numeric `>`. It is a *subtyping* relation: wherever a `&'b T` is
expected, a `&'a T` may be used when `'a: 'b`, because a longer-lived borrow is always a valid
shorter-lived one.

```rust
// two independent lifetimes, but 'a is known to cover 'b
fn pick<'a, 'b>(preferred: &'a str, fallback: &'b str) -> &'b str
where
    'a: 'b,
{
    if preferred.is_empty() { fallback } else { preferred } // &'a str coerces to &'b str
}
```

Drop the `where 'a: 'b` and the `preferred` branch fails: nothing tells the compiler that `'a`
covers `'b`. Note the signature is deliberately *weaker* than `fn pick<'a>(&'a str, &'a str) -> &'a str`
in what it returns and *more precise* in what it accepts — the single-lifetime version silently
unifies both inputs down to the shorter region, which is fine for `longest`-style helpers and
occasionally too coarse when the caller wants to keep using the longer one afterwards.

Three practical appearances:

**Type outlives a region — `T: 'a`.** Not "`T` lives for `'a`" but "`T` contains no borrow shorter
than `'a`". `String`, `i64` and `Arc<Config>` satisfy `T: 'a` for every `'a` because they hold no
borrows at all. This bound used to be written by hand on structs (`struct S<'a, T: 'a>`); modern
Rust infers it from the field types, which is why nothing in `lib/` spells it out.

**`'static` as a bound.** `T: 'static` means "`T` contains no non-`'static` borrow", which includes
every owned type — `String: 'static` is true. This is the bound `tokio::spawn` needs, and it is
the reason `__spawn_action` reads:

```rust
// lib/framework/src/task.rs:25
pub fn __spawn_action<T, R>(name: &'static str, location: &'static str, task: T)
where
    T: Future<Output = Result<R, Exception>> + Send + 'static,
    R: Send + Sync + 'static,
```

**`&'static T` is a different claim.** `&'static Config` is a reference that is valid forever;
`T: 'static` merely says the *type* has no short borrows. Confusing the two produces the single
most common "why does it want `'static`?" question. `app/demo/src/lib.rs` gets a real `&'static`
by leaking:

```rust
let state: &'static AppState = Box::leak(Box::new(AppState { db }));
```

and `lib/framework/src/string.rs` does the same for interned keys, so that `context!` and `stats!`
can take `&'static str` without ever cloning:

```rust
pub fn intern(value: &str) -> &'static str {
    // ... Box::leak(value.to_owned().into_boxed_str())
}
```

---

## 7. Lifetimes on structs

A struct with a lifetime parameter is a **view**: it may not outlive what it points at. The
workspace uses it for two purposes.

### 7.1 Zero-copy serialization payloads

```rust
// lib/framework/src/cloud/gcloud.rs:153
#[derive(Debug, Serialize)]
struct ActionEntry<'a> {
    id: &'a str,
    kind: &'a str,
    app: &'a str,
    error_message: Option<&'a str>,
    context: &'a [(String, Vec<String>)],
    severity: &'static str,
    // ...
}
```

Every field borrows from the `ActionMessage` being flushed; the entry is built, serialized to json
and dropped inside one function, so nothing has to be cloned on the logging hot path. Note the mix:
`severity` is `&'static str` because it comes from a `const` table
(`Severity::as_str() -> &'static str` in `lib/framework/src/log.rs:42`), while the rest are `'a`.
`app/log_processor_rs/src/alert.rs:24` uses the same pattern for `Alert<'a>`.

### 7.2 RAII guards

```rust
// lib/framework/src/metrics/counter.rs:11
pub struct CounterGuard<'a>(&'a Counter);

impl Drop for CounterGuard<'_> {
    fn drop(&mut self) {
        self.0.decrease();
    }
}

impl Counter {
    pub fn increase(&self) -> CounterGuard<'_> {
        // ...
        CounterGuard(self)
    }
}
```

The lifetime is load-bearing: the guard holds a borrow of the `Counter`, so the compiler guarantees
the counter cannot be dropped while a guard still exists to decrement it. `ResourceGuard<'a, R>` in
`lib/framework/src/pool.rs:111` is the bigger version — it borrows the pool (`pool: &'a ResourcePool<R>`)
and returns the resource to it on `Drop`.

Two notation points visible here:

- **`'_` in `impl` position.** `impl Drop for CounterGuard<'_>` means "for any lifetime". Writing
  `impl<'a> Drop for CounterGuard<'a>` is identical; `'_` is preferred when the name is unused. The
  workspace uses it consistently (`impl<R> Deref for ResourceGuard<'_, R>`).
- **`-> CounterGuard<'_>` in return position.** `'_` is mandatory rather than optional here: it
  makes the borrow from `&self` visible at the call site. Omitting it (`-> CounterGuard`) is a
  deny-by-default lint (`elided_lifetimes_in_paths` territory) precisely because the borrow would be
  invisible to a reader.

When a lifetime parameter is *only* needed to satisfy the type system and no field uses it, that is
`PhantomData` territory — see `Cond<E>` / `Update<E>` in `lib/framework_db/src/field.rs`, which use
`PhantomData<E>` for a type parameter for the same reason.

---

## 8. Higher-ranked bounds — `for<'a>`

Sometimes a bound must hold for *every* lifetime, not for one the caller chooses. That is a
higher-ranked trait bound (HRTB):

```rust
where F: for<'a> Fn(&'a str) -> &'a str
```

"`F` works no matter how long the input borrow is." Compare with `F: Fn(&'a str) -> &'a str` for a
fixed `'a`, which is a much weaker (and usually unusable) requirement — the caller would have to
commit to one region up front.

You rarely write `for<'a>` explicitly, because `Fn` sugar elides it: `Fn(&str) -> &str` already
desugars to the higher-ranked form. It shows up in this workspace implicitly:

```rust
// lib/framework_nats/src/service.rs:84
pub fn metrics(&self) -> impl Fn(&mut Metrics) + use<> {
```

`Fn(&mut Metrics)` is `for<'x> Fn(&'x mut Metrics)` — the returned closure accepts a `&mut Metrics`
borrow of any length, which is required because the metrics collector calls it on a fresh borrow at
every collection tick. `DeserializeOwned` (§4.3) is the same idea spelled as a trait alias.

---

## 9. `impl Trait` capture and `use<>` (edition 2024)

This workspace is on `edition = "2024"`, where **return-position `impl Trait` captures every
in-scope generic and lifetime parameter by default**, including the lifetime of `&self`. That is
the right default for most code — but it means a returned opaque type silently keeps `self`
borrowed:

```rust
pub fn metrics(&self) -> impl Fn(&mut Metrics) {   // captures &self's lifetime in 2024
    let counter = Arc::clone(&self.counter);
    move |metrics| { /* ... */ }
}
```

The closure does not actually use `self` — it clones an `Arc` up front — but the caller would still
be unable to move or mutate the service while holding the returned collector. `use<>` opts out
precisely:

```rust
// lib/framework_nats/src/service.rs:84
pub fn metrics(&self) -> impl Fn(&mut Metrics) + use<> {
```

`use<>` with an empty list means "**capture nothing**": no lifetime, no type parameter. The
returned value is independent of the receiver, so `NatsService::metrics()` can be called and its
result registered with the metrics registry while `start(self)` consumes the service. `use<'a, T>`
captures exactly the named parameters when you need something in between.

The related idiom for futures avoids opaque capture entirely by naming the bound:

```rust
// lib/framework/src/appender.rs:16
fn append_action(&self, action: ActionMessage) -> impl Future<Output = ()> + Send;
```

`+ Send` on a returned future is not a lifetime rule but it interacts with one: a future that holds
a non-`Send` borrow across an `await` fails this bound, which is how most "future is not `Send`"
errors in this codebase actually start.

---

## 10. Trait implementations with lifetimes

```rust
// lib/framework_db/src/types/timestamp.rs:50
impl<'a> FromSql<'a> for Timestamp {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> { /* ... */ }
}
```

`FromSql<'a>` is generic over the lifetime of the raw byte buffer the driver hands you. `Timestamp`
is an owned value that copies out of `raw`, so it implements the trait for *any* `'a` — that is
what `impl<'a> ... for Timestamp` (with `'a` unconstrained by `Self`) says. A type that kept a
`&'a [u8]` field would instead be `impl<'a> FromSql<'a> for Borrowed<'a>`, and would then be
pinned to the buffer. Either way the impl may not renegotiate the relation the trait declared —
§5.3 has the three ways an impl can get that wrong.

**Default lifetime bounds on trait objects** are worth knowing here too:

| Written | Means |
|---|---|
| `Box<dyn ToSql>` | `Box<dyn ToSql + 'static>` |
| `&'a dyn ToSql` | `&'a (dyn ToSql + 'a)` |
| `Box<dyn ToSql + 'a>` | explicitly allows borrowed contents |

`lib/framework_db/src/field.rs:49` spells out the default on purpose:

```rust
enum CondInner {
    Eq(Box<dyn ToSql + Sync + Send + 'static>),
    // ...
}
```

The `'static` is redundant to the compiler and informative to the reader: a condition value owns its
data and can therefore be stored, moved into a spawned task, or held across an `await`.

---

## 11. Compiler error decoder

| Error | What it actually means | Usual fix |
|---|---|---|
| **E0106** missing lifetime specifier | two or more input references, one output — elision cannot choose | annotate: `fn f<'a>(a: &'a T, b: &U) -> &'a V` (§4.1) |
| **E0515** cannot return reference to local variable | you are returning a borrow of something dropped at the closing brace | return an owned `String`/`Vec`, or take the buffer as a parameter |
| **E0597** `x` does not live long enough | the borrow outlives the owner | move the owner to an outer scope, or clone |
| **E0716** temporary value dropped while borrowed | borrowing out of a temporary in the same expression | bind it: `let s = format!(...); f(&s)` |
| **E0499** / **E0502** two mutable borrows / mutable + immutable | borrow-check, not lifetimes | split the borrow, restructure, or use indices |
| **E0521** borrowed data escapes outside of function | a borrow was passed to something requiring `'static` (usually `tokio::spawn`) | clone/own before spawning, or use `Arc` — §5.2, §6 |
| "future cannot be sent between threads safely" | a non-`Send` value (often a `MutexGuard`) is held across an `await` | drop the guard before awaiting; scope it in a block |

The two that trip people up most are E0515 and E0521, and both have the same underlying cause: an
attempt to return or move a *view* of data whose owner is about to disappear. Adding `'a` will not
help either one.

---

## 12. Rules of thumb used in this workspace

1. **Default to owned in public APIs that cross an async boundary.** Anything stored in an
   `ActionMessage`, sent over a channel, or captured by `spawn_action!` is owned. Borrowing is for
   synchronous, short-lived paths.
2. **`&'static str` for keys and constants.** `context!`, `stats!`, `error_code`, `location` and
   `kind` are all `&'static str` — they come from literals or `intern()` and cost nothing to copy.
3. **Borrow on the hot path where the buffer is obviously alive.** `ActionEntry<'a>`, `header<'a>`,
   `truncate_to_max` — build, serialize, drop, all in one function.
4. **Reach for `Arc` before reaching for a lifetime parameter on an async struct.** A struct with
   `'a` cannot be spawned; almost every shared thing in `lib/framework` is `Arc<T>` or a leaked
   `&'static`.
5. **Never add a lifetime parameter to make an error go away.** Decide the ownership first; the
   annotation then writes itself.
6. **Write the relation down whenever there is more than one reference parameter.** Elision picks a
   default that happens to compile in the body; only the signature reaches your callers (§5).

---

## 13. Exercises

Work in `app/demo/examples/` (the only crate without the workspace restriction lints, so `println!`
is allowed). Run with `cargo run -p demo --example <name>`.

**A. Elision.** Write `fn first_word(s: &str) -> &str` with no explicit lifetime. Then add a second
parameter `sep: &str` and explain the resulting E0106 before fixing it.

**B. `'a: 'b`.** Make this compile by adding one `where` clause, then explain in one sentence why
the clause is required:
```rust
fn choose<'a, 'b>(primary: &'a str, backup: &'b str) -> &'b str {
    if primary.len() > 3 { primary } else { backup }
}
```

**C. Guards.** Re-implement `CounterGuard` from `lib/framework/src/metrics/counter.rs` from scratch
against a plain `struct Counter { count: Cell<u32> }`. Then try to store the guard in a `Vec` that
outlives the counter and read the error.

**D. Zero-copy vs owned.** Define `struct Entry<'a> { app: &'a str, message: &'a str }` and a
mirrored `struct OwnedEntry { app: String, message: String }`. Write a function returning each from
a `String` buffer; observe that only the owned one compiles when the buffer is a local.

**E. Capture.** Write a method `fn collector(&self) -> impl Fn() -> u64` on a struct holding an
`Arc<AtomicU64>`, and then call `self.reset()` (taking `&mut self`) while holding the result.
Observe the error, fix it with `+ use<>`, and compare with
`NatsService::metrics()` at `lib/framework_nats/src/service.rs:84`.

**F. `'static` bound.** Take a `&str` local, try to move it into `spawn_action!`, read E0521, and
fix it two ways: `to_owned()` and `intern()`. Explain when each is appropriate.

**G. Two walls.** (a) Write the `label_elided` / `label_static` pair from §5.1 — same body, two
return types — and break a call site with one of them only. (b) Write the `Registry::add` example
from §5.2 and predict, before compiling, whether passing only string literals saves you. Then state
in one sentence why the answer differs from (a)'s call-site failure.

**H. Implementing a fixed signature.** Declare `trait Named { fn name(&self) -> &str; }` and
implement it for `struct Borrowed<'a> { name: &'a str }` three ways: `fn name(&'a self) -> &'a str`,
`fn name(&self) -> &str`, and `fn name(&'_ self) -> &'a str`. One fails to compile, two succeed, and
the two that succeed are indistinguishable to a caller. Explain why the third is legal and why it is
still pointless.

---

## 14. Further reading

- The Rust Book, ch. 10.3 (Validating References with Lifetimes) and ch. 19.2 (Advanced Lifetimes)
- The Rustonomicon, "Subtyping and Variance" — for why `&'a T` is covariant in `'a` and `&'a mut T`
  is not, which explains a class of otherwise baffling errors around `&mut` in structs
- The Reference, "Lifetime elision" — the three rules in normative form
- In this repo: `lib/framework/src/pool.rs`, `lib/framework/src/metrics/counter.rs`,
  `lib/framework_db/src/field.rs`, `lib/framework/src/cloud/gcloud.rs`
