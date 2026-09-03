# Session 1 — Ownership, Moves, Borrows, Lifetimes

Part of [Rust & Framework Training](00-outline.md) · Phase 1, Session 1 of 15

> **Instructor guide.** Everything here is meant to be followed live: the timings, the code to type,
> the compiler output to expect, and the questions to ask. Code blocks marked **[type this]** are
> written live in front of the group; blocks marked **[expected]** are what the terminal prints.

---

## At a glance

| | |
|---|---|
| **Goal** | Read and predict ownership/borrow errors without guessing. Recognise the four ownership patterns this framework uses. |
| **Duration** | 90 min |
| **Prerequisites** | Session 0 complete: `cargo clippy -- -D warnings` green, `cargo run -p demo --example action_log` works |
| **Participant prework** | The Rust Book, ch. 4 (all of it). ~40 min. |
| **Instructor prep** | 20 min — see checklist below |
| **Repo files used** | `lib/framework/src/string.rs`, `lib/framework/src/log/span.rs`, `lib/framework/src/metrics/counter.rs`, `app/demo/src/lib.rs`, `lib/framework/src/pool.rs` |

**What we are *not* covering today:** traits (Session 3), `Result`/`?` (Session 5), `Arc`/`Mutex`
and `Send`/`Sync` (Session 7). People will ask about all three. Park them on a visible list —
they are all scheduled, and answering them now costs 20 minutes you do not have.

---

## Timing

| Time | Segment | Mode |
|---|---|---|
| 0:00–0:10 | 1. Why this session exists, and the one-paragraph model | talk |
| 0:10–0:35 | 2. Live coding: move → borrow → `&str`/`String` → lifetime → `Drop` | live |
| 0:35–0:50 | 3. Four exhibits in the framework | read together |
| 0:50–1:25 | 4. Lab (pairs) | hands-on |
| 1:25–1:30 | 5. Wrap, pitfalls, homework | talk |

---

## Instructor prep checklist

- [ ] `git pull` on `main`; confirm `cargo build` is warm so live compiles take < 3 s.
- [ ] Create a scratch example file to type into:
      `app/demo/examples/training_s1.rs`, run with `cargo run -p demo --example training_s1`.
- [ ] **Know why the lab lives in `app/demo`:** that crate is the only one under `app/` *without*
      `[lints] workspace = true` in its `Cargo.toml`. The strict restriction lint set — including
      `print_stdout` — does not apply there, so lab code can use `println!` freely. Every other
      crate would reject it. Mention this once; it is the reason all Phase 1–2 labs go here.
- [ ] Have the five broken snippets from Exercise A saved and ready to paste (Appendix C).
- [ ] Terminal font ≥ 16pt. Compiler errors are the teaching material; everyone must be able to read them.

---

## 1. Opening (10 min)

### The framing to open with

> "In Java and TypeScript, you never think about who owns an object, because the garbage collector
> owns everything. That question doesn't go away in Rust — the compiler just makes you answer it,
> once, at compile time. Everything that feels strange today is that one question being asked in
> different places."

### The model, in one paragraph

Every value has exactly one **owner**. When the owner goes out of scope, the value is dropped —
freed, deterministically, at a point you can see in the source. Assigning or passing a value
**moves** ownership, unless the type is `Copy` (small, plain, no heap: integers, `bool`, `char`,
shared references). Instead of moving, you can **borrow**: `&T` is a shared read-only borrow, `&mut T`
is an exclusive read-write borrow. The rule the compiler enforces is *many readers XOR one writer*,
and a borrow may never outlive the value it points at. **Lifetimes** are the names we give those
borrow durations when the compiler cannot infer them.

### Draw this on the whiteboard and leave it up all session

```
                 owner                  borrows
  String   ──────────────► heap buffer ◄────── &str   (16 bytes: ptr + len)
  (24 bytes:                            ◄────── &str
   ptr + len + cap)

  owner dropped ──► buffer freed ──► any surviving borrow would dangle
                                     ^^^^ this is what the compiler prevents
```

### Ask the group (2 min, do not answer yet)

1. In Java, when is a `String` freed? *(Answer: unknowable — whenever the GC feels like it.)*
2. In Rust, when is a `String` freed? *(Answer: at the closing brace of its owner's scope. Point at
   the whiteboard.)*
3. Why might that determinism be worth the compile errors we are about to see?

Hold question 3 open. It gets answered at 0:31 when `Drop` shows up, and again in Session 8 when
graceful shutdown depends on it.

---

## 2. Live coding (25 min)

Type each of these into `app/demo/examples/training_s1.rs`, compile, read the error out loud, fix
it, move on. **Do not paste the fixed version first.** The compiler errors are the lesson.

### 2.1 — Move (5 min)

**[type this]**

```rust
fn main() {
    let name = String::from("neo");
    let upper = shout(name);
    println!("{name} -> {upper}");
}

fn shout(value: String) -> String {
    value.to_uppercase()
}
```

**[expected]**

```
error[E0382]: borrow of moved value: `name`
 --> e1.rs:4:16
  |
2 |     let name = String::from("neo");
  |         ---- move occurs because `name` has type `String`, which does not implement the `Copy` trait
3 |     let upper = shout(name);
  |                       ---- value moved here
4 |     println!("{name} -> {upper}");
  |                ^^^^ value borrowed here after move
  |
note: consider changing this parameter type in function `shout` to borrow instead if owning the value isn't necessary
 --> e1.rs:6:17
  |
6 | fn shout(value: String) -> String { value.to_uppercase() }
  |    -----        ^^^^^^ this parameter takes ownership of the value
help: consider cloning the value if the performance cost is acceptable
  |
3 |     let upper = shout(name.clone());
  |                           ++++++++
```

**Teach from the error itself.** Walk the four annotations in order: where it was created, why it
moves (`does not implement the Copy trait`), where it moved, where the use-after-move is.

Then the crucial point: **rustc offers two fixes, and the order is misleading.**

| Fix | When it is right |
|---|---|
| `fn shout(value: &str)` | Almost always. The function only reads. |
| `shout(name.clone())` | When the callee genuinely needs its own copy. |

> "rustc suggests `.clone()` because it is the fix that always compiles, not the fix that is usually
> correct. This workspace denies `implicit_clone`, `inefficient_to_string`, `str_to_string` and
> `assigning_clones` precisely because reflexive cloning is the habit newcomers form here. When you
> reach for `.clone()`, say out loud why the callee needs its own copy. If you can't, change the
> signature instead."

Fix it by changing the parameter to `&str`, recompile, green. Then ask: *why did `name` stay usable
this time?*

### 2.2 — The borrow rules (5 min)

**[type this]**

```rust
fn main() {
    let mut logs = String::from("start");
    let first = &logs;
    logs.push_str(" more");
    println!("{first}");
}
```

**[expected]**

```
error[E0502]: cannot borrow `logs` as mutable because it is also borrowed as immutable
 --> e2.rs:4:5
  |
3 |     let first = &logs;
  |                 ----- immutable borrow occurs here
4 |     logs.push_str(" more");
  |     ^^^^^^^^^^^^^^^^^^^^^^ mutable borrow occurs here
5 |     println!("{first}");
  |                ----- immutable borrow later used here
```

Three questions for the group:

1. Why is this rejected even though it is single-threaded? *(Because `push_str` may reallocate the
   buffer, leaving `first` pointing at freed memory. Show it on the whiteboard diagram.)*
2. What is the Java equivalent bug? *(`ConcurrentModificationException` — except Java finds it at
   runtime, sometimes, in production. Rust finds it at compile time, always.)*
3. Delete the `println!` and recompile. It passes. Why? *(Non-lexical lifetimes: the borrow ends at
   its last use, not at the closing brace. This surprises people who read older Rust material.)*

### 2.3 — `String` vs `&str` (7 min) — the highest-value 7 minutes of the session

**[type this]**

```rust
fn main() {
    println!("&str         = {} bytes (ptr + len)", size_of::<&str>());
    println!("String       = {} bytes (ptr + len + cap)", size_of::<String>());
    println!("&String      = {} bytes", size_of::<&String>());
    println!("Vec<u8>      = {} bytes", size_of::<Vec<u8>>());
    println!("Option<&str> = {} bytes (niche, same as &str)", size_of::<Option<&str>>());
    println!("i64          = {} bytes (Copy)", size_of::<i64>());
}
```

**[expected]**

```
&str         = 16 bytes (ptr + len)
String       = 24 bytes (ptr + len + cap)
&String      = 8 bytes
Vec<u8>      = 24 bytes
Option<&str> = 16 bytes (niche, same as &str)
i64          = 8 bytes (Copy)
```

The table to put on the board:

| | owns the bytes? | can grow? | size of the value itself | Java analogue |
|---|---|---|---|---|
| `String` | yes | yes | 24 B | `StringBuilder` you own |
| `&str` | no | no | 16 B (fat pointer) | a view — no real analogue |
| `&'static str` | no (static) | no | 16 B | a string literal in the constant pool |
| `&String` | no | no | 8 B | almost always wrong — use `&str` |

Two rules to state plainly and repeat all program long:

- **Take `&str` in parameters. Return `String` when you must allocate, `&str` when you can borrow.**
- **`&String` in a parameter is a code smell.** `&str` accepts both a `&String` (via deref coercion)
  and a literal; `&String` accepts only the former.

Also note `Option<&str>` is the same 16 bytes as `&str` — the *niche optimisation*. There is no
`null`, and `Option` is free here. That answers the "isn't `Option` slower than `null`?" question
before it is asked.

### 2.4 — Lifetimes are a description, not a mechanism (4 min)

**[type this]**

```rust
fn build() -> &str {
    let owned = String::from("hello");
    &owned
}
```

**[expected]**

```
error[E0106]: missing lifetime specifier
 --> e3.rs:5:15
  |
5 | fn build() -> &str {
  |               ^ expected named lifetime parameter
  |
  = help: this function's return type contains a borrowed value, but there is no value for it to be borrowed from
help: consider using the `'static` lifetime, but this is uncommon unless you're returning a borrowed value from a `const` or a `static`
  |
5 | fn build() -> &'static str {
  |                +++++++
help: instead, you are more likely to want to return an owned value
  |
5 - fn build() -> &str {
5 + fn build() -> String {
  |
```

Read the `help:` line aloud — *"there is no value for it to be borrowed from"* — it is the clearest
one-sentence definition of a lifetime rustc ever prints.

**The single most important thing to say about lifetimes:**

> "A lifetime annotation never changes what the program does at runtime. It is not a keep-alive, not
> a reference count, not a hint to an allocator. It is you *describing* a relationship that already
> exists in the code, so the compiler can check it. If adding `'a` fixes your bug, the bug was in
> your description, not in your memory management."

Then show that most lifetimes are inferred. This compiles with no annotation at all:

```rust
fn first_word(line: &str) -> &str {
    match line.find(' ') {
        Some(index) => &line[..index],
        None => line,
    }
}
```

*(Lifetime elision: one input reference, so the output borrows from it. State the rule, don't derive it.)*

Point at `lib/framework/src/metrics/counter.rs:11` on screen for a case where it must be written:

```rust
pub struct CounterGuard<'a>(&'a Counter);
```

> "A struct holding a reference must name the lifetime, because the compiler has to know the guard
> cannot outlive the counter it points at. That's the whole meaning of `'a` here."

### 2.5 — `Drop` and RAII (4 min)

**[type this]**

```rust
struct Timer {
    name: &'static str,
    start: std::time::Instant,
}

impl Timer {
    fn new(name: &'static str) -> Self {
        println!("[{name}] >");
        Timer { name, start: std::time::Instant::now() }
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        println!("[{}] elapsed={:?} <", self.name, self.start.elapsed());
    }
}

fn main() {
    {
        let _outer = Timer::new("outer");
        {
            let _inner = Timer::new("inner");
            println!("working");
        }
        println!("after inner");
    }
    println!("after outer");
}
```

**[expected]**

```
[outer] >
[inner] >
working
[inner] elapsed=750ns <
after inner
[outer] elapsed=20.167µs <
after outer
```

This is the payoff for the determinism question from the opening. Ask: *where is the `finally`
block?* There isn't one — scope exit **is** the finally block, it cannot be forgotten, and it runs
on the early-return and panic paths too.

Then say: **you have just written a simplified `span!`.** Open `lib/framework/src/log/span.rs:59`
side by side. Same shape, real implementation. That is the bridge into the next segment.

---

## 3. Four exhibits in the framework (15 min)

Open each file on screen. ~4 minutes each. The goal is recognition, not mastery — participants
should leave able to say "I've seen that pattern" when they meet it again in Phase 3.

### Exhibit 1 — borrow in, borrow out
**`lib/framework/src/string.rs:11`** — `fn truncate_to_max(&self, len: usize) -> &str`

```rust
impl StringExt for str {
    #[inline]
    fn truncate_to_max(&self, len: usize) -> &str {
        if len >= self.len() {
            return self;
        }
        let mut new_len = len;
        while new_len > 0 && !self.is_char_boundary(new_len) {
            new_len -= 1;
        }
        &self[..new_len]
    }
}
```

Ask:
- **How many allocations does this perform?** *(Zero. Both return paths hand back a view into the
  caller's buffer.)*
- **Why does the returned `&str` need no lifetime annotation?** *(Elision — one input reference.)*
- **What is the `is_char_boundary` loop for?** *(`&self[..n]` panics if `n` splits a UTF-8 character.
  Show the test at line 50: `"123老虎456".truncate_to_max(4) == "123"` — byte 4 is mid-`老`.)*
- **What would the Java version return?** *(A new `String` — an allocation and a copy.)*

Connect it to the framework: this is called on the action log's hot path, on every log line that
exceeds the cap. Zero-allocation truncation is the reason the function exists in this shape.

### Exhibit 2 — RAII with a side effect
**`lib/framework/src/log/span.rs:59`** — `impl Drop for Span`

```rust
impl Drop for Span {
    fn drop(&mut self) {
        let _result = CURRENT_ACTION.try_with(|action| {
            let mut action = action.borrow_mut();
            let name = self.name;
            let span_elapsed = self.start_time.elapsed();
            action.log(None, None, None, format_args!("[span:{name}] elapsed={span_elapsed:?} <"));
            action.add_stat(self.elapsed_key, span_elapsed.as_nanos() as u64);
            action.add_stat(self.count_key, 1);
        });
    }
}
```

Point out the call site convention seen throughout `framework_db`:

```rust
let _span = span!("db");
```

- **Why `let _span = ...` and not `let _ = ...`?** This is the question that catches everyone.
  `let _ = expr` drops the value **immediately**; `let _span = expr` binds it and drops at scope end.
  With `let _`, the span would measure nothing. Demo it live if there is time — it is a real bug
  people write.
- **Why does the timing work without any `finally`?** Because `Drop` runs on every exit path.
- Note `Span::clear()` at line 41 — Drop is not the only trick; the span also remembers a byte
  offset into the log buffer so a hot loop can roll its own trace back.

### Exhibit 3 — a lifetime you must write
**`lib/framework/src/metrics/counter.rs:11`** — `CounterGuard<'a>`

```rust
pub struct CounterGuard<'a>(&'a Counter);

impl Drop for CounterGuard<'_> {
    fn drop(&mut self) {
        self.0.decrease();
    }
}

impl Counter {
    pub fn increase(&self) -> CounterGuard<'_> {
        let current = self.count.fetch_add(1, Ordering::Relaxed) + 1;
        self.max.fetch_max(current, Ordering::Relaxed);
        CounterGuard(self)
    }
}
```

- The struct holds a borrow, so the lifetime is named. `'_` in the impls means "infer it".
- `increase()` returns a guard tied to `&self` — the guard **cannot** outlive the counter, and the
  compiler enforces it. In Java you would document this and hope.
- Call site, in `lib/framework/src/web/server.rs`: `let _counter = counter.increase();` — same
  `let _name` convention, same reason.

### Exhibit 4 — deliberately leaking, and why it is not a bug
**`app/demo/src/lib.rs:55`**

```rust
let state: &'static AppState = Box::leak(Box::new(AppState { db }));
```

and **`lib/framework/src/string.rs:40`**

```rust
let leaked: &'static str = Box::leak(value.to_owned().into_boxed_str());
```

This one always provokes a reaction from a Java audience. Handle it head-on:

> "`Box::leak` gives up ownership of a heap allocation forever, and hands you a `&'static` reference
> in exchange. That is a leak in the literal sense. It is correct here because both of these live
> exactly as long as the process: `AppState` is the app singleton, and interned strings are a
> bounded set of log keys. Leaking N things once at startup is not a leak in the sense that matters
> — leaking N things *per request* is."

- **Why not `Arc<AppState>`?** You could — `app/log_processor_rs/src/main.rs` does exactly that,
  because it needs to hand a clone to each of two consumers. `&'static` costs no refcount traffic
  and no `Arc::clone` at every call site. Both patterns are in the repo on purpose; Session 7
  compares all four.
- Note the open question in `TODO.md`: *"always make state with `Box::leak()`?"* — the team has not
  settled this. Say so. Trainees should know which conventions are firm and which are live.

---

## 4. Lab (35 min, in pairs)

### Setup (2 min)

```bash
touch app/demo/examples/training_s1.rs
```

Run it with:

```bash
cargo run -p demo --example training_s1
```

Remind them: `app/demo` has no `[lints] workspace = true`, so `println!` is fine here and only here.

### Exercise A — Fix five borrow errors (15 min)

Paste the five snippets from **Appendix C** into their file one at a time. For each one, the pair
must, **in this order**:

1. Read the error and say out loud *which rule* was broken, before touching the code.
2. Name at least two possible fixes.
3. Pick one and justify it in a sentence.

The error codes they will meet: `E0382` (use after move, ×2), `E0502` (mutable + immutable borrow,
×2), `E0106` (missing lifetime). Snippet 5 is the sneaky one — `for item in items` moves the vector
via `into_iter()`, and rustc's own note says so.

**Instructor note:** circulate and listen for step 1. A pair that jumps to the fix has learned to
appease the compiler, not to read it. The habit you are building today is *read the error, then act*.

### Exercise B — Borrow in, borrow out (10 min)

Write both, and make both compile:

```rust
fn first_word(line: &str) -> &str;
fn first_word_owned(line: &str) -> String;
```

Then answer, in a comment above each:

- How many heap allocations does each perform?
- Which one can be called on a `&'static str` literal? *(Both.)*
- Which one's result stays valid after the input `String` is dropped? *(Only `first_word_owned`.
  Prove it — write the code that fails, and read the error.)*

Reference solution in Appendix D.

### Exercise C — Write a guard (8 min)

Write the `Timer` from segment 2.5 from memory. Then, without looking at `span.rs`:

- Add a counter that reports how many timers are currently alive.
- Change `let _timer = Timer::new("x")` to `let _ = Timer::new("x")` and explain the output change.

### Stretch / homework

Read `lib/framework/src/pool.rs`, lines 111–160 — `ResourceGuard<'a, R>`. Answer in writing:

1. It holds **three** things that do work on drop: the resource itself, `_permit`, and `_counter`.
   What does each release, and in what order? *(Struct fields drop in declaration order, after the
   explicit `Drop::drop` body runs.)*
2. Why is the field `resource: Option<Resource<..>>` rather than `Resource<..>`? *(`drop(&mut self)`
   only gets a mutable borrow, so it cannot move the resource out — `Option::take` is the standard
   way to move a value out of a struct that is being dropped.)*
3. What happens to a connection whose `created_time.elapsed()` exceeds `max_life_time`? *(It is not
   pushed back into storage — it is dropped and closed.)*

Bring answers to Session 2.

---

## 5. Wrap-up (5 min)

### The four ownership patterns in this codebase — preview the map

| Pattern | Used for | Example | Covered in |
|---|---|---|---|
| Owned value moved in | data handed off for good | `repository::insert(&db, &user)` args | today |
| `&T` borrow | read-only access | everything taking `&str` | today |
| `&'static` via `Box::leak` | app singletons | `app/demo/src/lib.rs:55` | today / Session 7 |
| `Arc<T>` | shared across tasks | `app/log_processor_rs/src/main.rs` | Session 7 |

### Pitfalls to restate before they leave

1. **`.clone()` is not a fix, it is a decision.** Say why the callee needs its own copy, or change
   the signature. `implicit_clone`, `assigning_clones`, `str_to_string`, `inefficient_to_string`,
   `clone_on_ref_ptr` are all denied in this workspace.
2. **`&String` in a parameter is almost always wrong.** Use `&str`.
3. **`let _ = guard` drops immediately; `let _guard = guard` drops at scope end.** This silently
   breaks spans, counters and pool guards. Ten seconds to prove it, and worth showing:

   ```rust
   struct T(&'static str);
   impl Drop for T { fn drop(&mut self) { println!("drop {}", self.0); } }

   fn main() {
       let _ = T("anonymous");
       println!("-- after let _ --");
       let _named = T("named");
       println!("-- after let _named --");
   }
   ```

   ```
   drop anonymous
   -- after let _ --
   -- after let _named --
   drop named
   ```

   A `span!` bound with `let _` measures nothing; a pool guard bound with `let _` returns the
   connection before you have used it.
4. **A lifetime annotation changes nothing at runtime.** If you are adding `'a` to fix a crash, stop
   — you have a design problem, not an annotation problem.
5. **Non-lexical lifetimes:** a borrow ends at its last *use*, not at the closing brace. Half the
   "why does this compile?" surprises are this.

### Homework

- Pool guard questions above.
- **Prework for Session 2:** the Rust Book ch. 5 and 6, and skim
  `lib/framework_db/src/field.rs` — specifically `enum CondInner` and `build_conditions`.

### Exit ticket (one line, in the group chat)

> "Name one place in the framework where `Drop` does real work, and say what would break without it."

---

## Appendix A — Cheat sheet to hand out

| You want to… | Write |
|---|---|
| Read a string parameter | `fn f(s: &str)` |
| Read and keep a string | `fn f(s: String)` or `fn f(s: &str) -> String { s.to_owned() }` |
| Modify in place | `fn f(s: &mut String)` |
| Return a view of the input | `fn f(s: &str) -> &str` |
| Return a new string | `fn f(...) -> String` |
| A value that lives for the process | `&'static T` via `Box::leak`, or a `const`/`static` |
| Run code at scope exit | `impl Drop for T`, bound as `let _name = ...` |
| Copy a small plain value | nothing — `i32`, `bool`, `char`, `&T` are `Copy` |

**Error code decoder:**

| Code | Means | Usual fix |
|---|---|---|
| `E0382` | use after move | borrow instead of moving, or restructure |
| `E0502` | mutable + immutable borrow overlap | shorten the immutable borrow's last use |
| `E0499` | two mutable borrows | split the scope, or split the struct |
| `E0106` | missing lifetime specifier | return an owned value, or name the input it borrows from |
| `E0597` | borrowed value does not live long enough | the owner is dropped too early — hoist it |
| `E0507` | cannot move out of borrowed content | `.clone()`, `.take()`, or restructure |

## Appendix B — Questions this group will ask

**"Why not just `.clone()` everywhere and move on?"**
You can, and for a startup script you should. Here it costs allocations on paths that run per log
line and per request, and the workspace lints are configured to push back. More importantly, the
clone habit hides the design question — *who owns this?* — which you will have to answer anyway when
you hit Session 7 and the value must cross a task boundary.

**"Is `&T` a pointer? Can it be null?"**
It is a pointer at runtime. It can never be null and never dangle — that is what the borrow checker
buys. Absence is `Option<&T>`, which (see the `size_of` demo) costs nothing extra.

**"Does Rust have a GC? When is memory freed?"**
No GC. Freed when the owner goes out of scope, at a point visible in the source. That is why `Drop`
can do real work like emitting a log span or returning a connection to a pool — you know exactly
when it runs.

**"Then why does `String` exist at all, if `&str` is cheaper?"**
Someone has to own the bytes. `&str` is only a view of a buffer owned elsewhere. `String` is the
owner; `&str` is how you pass it around without copying.

**"Is a lifetime like a reference count?"**
No. Zero runtime cost, zero runtime representation — the annotation is erased after type checking.
It only lets the compiler verify a relationship that is already true in your code.

**"`Box::leak` is a memory leak. Why is that in the codebase?"**
Because leaking a fixed number of objects once, at startup, for the life of the process, is not the
failure mode the word "leak" warns about. Look at what is leaked: the app state singleton and a
bounded set of interned log keys. Both would live until exit regardless. See `TODO.md` — the team is
still deciding whether to make it the universal convention.

**"What's the equivalent of `final`?"**
Bindings are immutable by default; `mut` is opt-in. So Rust is "final by default", the inverse of
Java. There is no `final` keyword because there is nothing to add.

**"How does this relate to `synchronized` / thread safety?"**
Directly — the same *many readers XOR one writer* rule is what makes data races impossible across
threads. That is Session 7. Add it to the parked list.

## Appendix C — Exercise A snippets

```rust
// --- 1 ---
fn main() {
    let name = String::from("neo");
    let upper = shout(name);
    println!("{name} -> {upper}");
}
fn shout(value: String) -> String { value.to_uppercase() }

// --- 2 ---
fn main() {
    let mut logs = String::from("start");
    let first = &logs;
    logs.push_str(" more");
    println!("{first}");
}

// --- 3 ---
fn main() {
    let line = build();
    println!("{line}");
}
fn build() -> &str {
    let owned = String::from("hello");
    &owned
}

// --- 4 ---
struct Action { logs: String }
impl Action {
    fn log(&mut self, message: &str) { self.logs.push_str(message); }
    fn last(&self) -> &str { &self.logs }
}
fn main() {
    let mut action = Action { logs: String::new() };
    let last = action.last();
    action.log("another line");
    println!("{last}");
}

// --- 5 ---
fn main() {
    let items = vec![String::from("a"), String::from("b")];
    for item in items {
        println!("{item}");
    }
    println!("count={}", items.len());
}
```

**Answer key**

| # | Error | Rule broken | Best fix | Also acceptable |
|---|---|---|---|---|
| 1 | `E0382` | use after move | `fn shout(value: &str)` | `shout(name.clone())` — must justify |
| 2 | `E0502` | writer while a reader is live | move `println!("{first}")` before `push_str` | `let first = logs.clone()` — worse |
| 3 | `E0106` | returning a borrow of a local | return `String` | make the input a parameter and borrow from it |
| 4 | `E0502` | `&mut self` while `&self` borrow is live | print `last` before `log`, or re-read after | clone the returned `&str` |
| 5 | `E0382` | `for` loop moved the `Vec` via `into_iter()` | `for item in &items` | move `items.len()` above the loop |

Snippet 4 is the one that matters most — it is `Action` in miniature, and it is exactly why
`lib/framework/src/log/action.rs` keeps its log buffer behind `&mut self` methods rather than
handing out `&str` views.

## Appendix D — Reference solutions

```rust
// Exercise B
// zero allocations: returns a view into the caller's buffer
fn first_word(line: &str) -> &str {
    match line.find(' ') {
        Some(index) => &line[..index],
        None => line,
    }
}

// one allocation: copies the view into a buffer the caller owns
fn first_word_owned(line: &str) -> String {
    first_word(line).to_owned()
}

// Exercise C
struct Timer {
    name: &'static str,
    start: std::time::Instant,
}

impl Timer {
    fn new(name: &'static str) -> Self {
        println!("[{name}] >");
        Timer { name, start: std::time::Instant::now() }
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        println!("[{}] elapsed={:?} <", self.name, self.start.elapsed());
    }
}

fn main() {
    let line = String::from("hello rust world");

    let word = first_word(&line);
    println!("borrowed: {word}");

    let owned = first_word_owned(&line);
    println!("owned: {owned}");

    println!("line still usable: {line}");

    {
        let _outer = Timer::new("outer");
        {
            let _inner = Timer::new("inner");
            println!("working");
        }
        println!("after inner");
    }
    println!("after outer");
}
```

**[expected]**

```
borrowed: hello
owned: hello
line still usable: hello rust world
[outer] >
[inner] >
working
[inner] elapsed=750ns <
after inner
[outer] elapsed=20.167µs <
after outer
```

The "prove it" step of Exercise B — this must **not** compile:

```rust
let word;
{
    let line = String::from("hello world");
    word = first_word(&line);   // borrows `line`
}                               // `line` dropped here
println!("{word}");             // E0597: `line` does not live long enough
```

**[expected]**

```
error[E0597]: `line` does not live long enough
  --> prove.rs:8:27
   |
 7 |         let line = String::from("hello world");
   |             ---- binding `line` declared here
 8 |         word = first_word(&line);
   |                           ^^^^^ borrowed value does not live long enough
 9 |     }
   |     - `line` dropped here while still borrowed
10 |     println!("{word}");
   |                ---- borrow later used here
```

---

**Next:** Session 2 — Structs, enums, pattern matching, `Option`.
