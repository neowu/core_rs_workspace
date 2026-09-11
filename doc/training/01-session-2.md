# Session 2 — Futures, `async`/`await`, Tokio, and `Pin`

Part of [Rust & Framework Training](00-outline.md) · Phase 2

> **Instructor guide.** Everything here is meant to be followed live: the timings, the code to type,
> the compiler output to expect, and the questions to ask. Code blocks marked **[type this]** are
> written live in front of the group; blocks marked **[expected]** are what the terminal prints.
> Every output block in this document was produced by running the code on this workspace
> (rustc stable, edition 2024, tokio 1.53.1).

> **Ordering note.** [`00-outline.md`](00-outline.md) schedules this material as **Session 6**, after
> the Phase 1 language sessions. This document is written to be teachable there. If you run it
> earlier — as Session 2, straight after ownership — skip §5 (`ActionFuture`) and Exercise C, which
> assume traits and generics from outline Session 3. Everything else only needs Session 1.

---

## At a glance

| | |
|---|---|
| **Goal** | Understand that a future is an inert, self-referential state machine that someone must poll — and be able to explain, from the code, why that forces `Pin` into every signature. |
| **Duration** | 90 min |
| **Prerequisites** | Session 1 (ownership, borrows, `Drop`). Ideally also traits/generics. |
| **Participant prework** | Tokio tutorial, "Async in depth" chapter. The Rust Book ch. 17. ~45 min. |
| **Instructor prep** | 30 min — see checklist below |
| **Repo files used** | `app/demo/src/main.rs`, `app/demo/src/lib.rs`, `lib/framework/src/log.rs`, `lib/framework/src/appender.rs`, `lib/framework/src/task.rs`, `doc/action_future_design.md` |

**What we are *not* covering today:** `Send`/`Sync`, `Arc`, `Mutex` (Session 7);
`CancellationToken`, `TaskTracker`, graceful shutdown (Session 8). People will ask about all of
them — today's cancellation segment is deliberately only "a dropped future stops running". Park the
rest on a visible list.

---

## Timing

| Time | Segment | Mode |
|---|---|---|
| 0:00–0:08 | 1. Why async exists here, and the one-paragraph model | talk |
| 0:08–0:28 | 2. A future is inert: drive one by hand, with no runtime | live |
| 0:28–0:45 | 3. How `async fn` becomes a state machine | live + whiteboard |
| 0:45–1:00 | 4. Why the state machine needs `Pin` | live |
| 1:00–1:10 | 5. The framework exhibit: `ActionFuture` and the size multiplier | read together |
| 1:10–1:25 | 6. Lab (pairs) | hands-on |
| 1:25–1:30 | 7. Wrap, pitfalls, homework | talk |

---

## Instructor prep checklist

- [ ] `git pull` on `main`; `cargo build -p demo` so live compiles stay under 3 s.
- [ ] Create the scratch file you will type into:
      `app/demo/examples/training_s2.rs`, run with `cargo run -p demo --example training_s2`.
- [ ] **Know why the lab lives in `app/demo`:** it is the only crate under `app/` *without*
      `[lints] workspace = true`, so `println!` and `unsafe` are allowed there. Section 4 needs
      both. Say this once — every other crate would reject the code you are about to write.
- [ ] Read [`doc/action_future_design.md`](../action_future_design.md) end to end. Section 5 is a
      guided read of it; you should be able to answer questions off-book.
- [ ] Run the size measurement in §5.3 yourself first — the exact numbers depend on the rustc
      version, and you want to quote what *your* machine prints, not what this document does.
- [ ] Have Appendix C's four broken snippets ready to paste.
- [ ] Terminal font ≥ 16pt. The `Unpin` errors are long; everyone must be able to read them.

---

## 1. Opening (8 min)

### The framing to open with

> "In Java you block a thread and the platform gives you more threads. In JavaScript you never
> block, because there is only one thread and the event loop owns it. Rust picked a third answer:
> a `Future` is a **value you own** that makes progress only when something calls `poll` on it. No
> thread is parked, no event loop is hidden, and the thing doing the polling — the runtime — is a
> library you added to `Cargo.toml`, not part of the language."

Point at [`app/demo/src/main.rs`](../../app/demo/src/main.rs). It is four lines and every one of
them matters today:

```rust
#[tokio::main]
async fn main() {
    demo::run().await;
}
```

`async fn main` cannot be a real `main` — `main` returns `()`, and an `async fn` returns a future.
`#[tokio::main]` rewrites it into a synchronous `main` that builds a runtime and hands it that
future to poll. **Everything async in this repo bottoms out in that one call.**

### The model, in one paragraph

An `async fn` does not run code. It **builds a value** — a state machine — and returns it. That
value is inert: constructing it allocates nothing on the heap, starts no thread, and executes none
of the body. Someone must call `Future::poll` on it. `poll` runs the body up to the next `.await`
that cannot yet finish, then returns `Poll::Pending` after arranging for a **waker** to be called
when progress becomes possible; or it returns `Poll::Ready(value)` and it is done. The **runtime**
(tokio) is the code that owns a queue of futures, polls them, and re-queues the ones that get woken.
Because the state machine must remember locals across an `.await`, and because those locals can
**borrow each other**, the machine is often self-referential — which is why `poll` takes
`Pin<&mut Self>` and not `&mut Self`.

### Draw this on the whiteboard and leave it up all session

```
   async fn body            the value it returns              who calls poll
   ─────────────            ────────────────────              ──────────────
   let a = f();             enum MyFuture {                   tokio runtime
   g().await;                 Start,                          ├─ ready queue
   let b = h(a);              WaitingOnG { a, awaitee },  ◄── ├─ worker threads
   b                          Done,                           └─ timer / io driver
                            }                                        │
                                                             poll(Pin<&mut F>, cx)
        nothing runs   ──►   still nothing runs   ──►   NOW the body runs
        at call                at construction            (up to the next await)
```

### Ask the group (2 min, do not answer yet)

1. In TypeScript, when does the body of `async function f()` start running? *(At the call. The
   promise is already in flight.)*
2. In Rust, when does the body of `async fn f()` start running? *(At the first `poll` — which for
   most code means at the `.await`.)*
3. What can you do with a Rust future that you cannot do with a JS promise? *(Throw it away and
   have the work simply not happen. Hold that thought until 0:58.)*

---

## 2. A future is inert (20 min)

### 2.1 — Laziness (5 min)

**[type this]** into `app/demo/examples/training_s2.rs`:

```rust
use std::time::Duration;
use std::time::Instant;

use tokio::time::sleep;

async fn work(name: &str) -> u32 {
    println!("  [{name}] body started");
    sleep(Duration::from_millis(50)).await;
    println!("  [{name}] body finished");
    7
}

#[tokio::main]
async fn main() {
    let started = Instant::now();
    let future = work("a");
    println!("  future created, elapsed={:?}", started.elapsed());
    println!("  size_of future = {}", size_of_val(&future));
    let value = future.await;
    println!("  awaited, value={value}");
}
```

**[expected]**

```
  future created, elapsed=41ns
  size_of future = 152
  [a] body started
  [a] body finished
  awaited, value=7
```

Three things to say out loud, in this order:

1. **"body started" printed *after* "future created".** The `println!` on the first line of `work`
   is inside the state machine. Constructing the machine does not run it.
2. **The future is 152 bytes, on the stack.** It is a plain value. No `Box`, no allocation, no
   thread. `size_of_val` on a future is a habit worth forming today — §5 is entirely about it.
3. **A JS reader would expect the opposite.** In TS, `work("a")` starts immediately and `await` just
   subscribes to the result. Here, `.await` *is* what makes it run.

The consequence, in one sentence: **a future you never `.await` is a future that never ran.** Clippy
catches the obvious case (`#[must_use]` on `Future`), but not, for example, building a future in a
`match` arm and dropping it.

### 2.2 — Concurrency comes from the runtime, not from `async` (5 min)

**[type this]**

```rust
let started = Instant::now();
let _ = work("b").await;
let _ = work("c").await;
println!("  sequential elapsed={:?}", started.elapsed());

let started = Instant::now();
let (_x, _y) = tokio::join!(work("d"), work("e"));
println!("  join elapsed={:?}", started.elapsed());
```

**[expected]**

```
  [b] body started
  [b] body finished
  [c] body started
  [c] body finished
  sequential elapsed=104.075042ms
  [d] body started
  [e] body started
  [e] body finished
  [d] body finished
  join elapsed=51.336125ms
```

Ask: *why is the sequential version 100 ms?* Because `.await` means "poll this until it is Ready
before continuing" — it is as sequential as any blocking call. `async` alone buys you **nothing**;
concurrency appears only when something polls more than one future. `join!` does that on one task;
`tokio::spawn` does it on possibly-different threads.

Note the interleaving on the `join!` line: `d` and `e` both start, then `e` finishes first. `join!`
polls its branches in a loop in a single task — there is no parallelism here, just interleaving at
`.await` points.

### 2.3 — Poll it yourself, with no runtime at all (10 min)

This is the centre of the session. **Do not skip it.**

**[type this]**

```rust
use std::future::Future;
use std::pin::Pin;
use std::pin::pin;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

struct Countdown {
    remaining: u32,
}

impl Future for Countdown {
    type Output = &'static str;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        println!("  poll, remaining={}", self.remaining);
        if self.remaining == 0 {
            return Poll::Ready("liftoff");
        }
        self.remaining -= 1;
        cx.waker().wake_by_ref();   // "I could make progress again immediately"
        Poll::Pending
    }
}
```

and drive it in `main` **without tokio doing the driving**:

```rust
let mut future = pin!(Countdown { remaining: 2 });
let mut cx = Context::from_waker(Waker::noop());
loop {
    match future.as_mut().poll(&mut cx) {
        Poll::Pending => println!("  got Pending, loop again"),
        Poll::Ready(value) => {
            println!("  got Ready({value})");
            break;
        }
    }
}
```

**[expected]**

```
  poll, remaining=2
  got Pending, loop again
  poll, remaining=1
  got Pending, loop again
  poll, remaining=0
  got Ready(liftoff)
```

Walk the group through the `Future` trait itself, which is now fully on screen:

```rust
pub trait Future {
    type Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output>;
}

pub enum Poll<T> { Ready(T), Pending }
```

That is the entire trait. Everything else in async Rust — `async fn`, `.await`, `join!`, `select!`,
tokio itself — is built on those five lines.

Four points, one per question the group will have:

- **`cx: &mut Context`'s only job is to carry a `Waker`.** Returning `Pending` without arranging for
  the waker to be called is how a future hangs forever. Our `Countdown` cheats by waking itself
  immediately; a real leaf future (a timer, a socket) hands the waker to the OS event source and
  returns.
- **A `Waker` is not "resume this coroutine".** It is "put the *task* that owns this future back on
  the runtime's ready queue", which polls the whole future tree again from the top. This is why
  `poll` must be cheap and re-entrant.
- **`Poll::Pending` is a return, not a yield.** The stack is gone. Anything the body needs after the
  await must have been saved into the future's own memory. That is the whole reason the next section
  exists.
- **`self: Pin<&mut Self>`, not `&mut Self`.** Nobody has explained this yet. Write "why `Pin`?" on
  the board and point at it — it gets answered at 0:45.

**Then delete the hand-rolled loop and say:** what you just wrote *is* a runtime, for one future,
with a busy-wait instead of a ready queue. Tokio is this loop plus a work-stealing scheduler, an
epoll/kqueue driver, and a timer wheel.

---

## 3. How an `async fn` becomes a state machine (17 min)

### 3.1 — The desugaring, on the whiteboard (5 min)

Write this on the left:

```rust
async fn two_step() -> u32 {
    let total = 40;                 // local, created before the await
    sleep(20.ms()).await;           // suspension point
    total + 2                       // local, still needed after the await
}
```

and this on the right:

```rust
enum TwoStep {
    Start,
    WaitingOnSleep { sleeping: Sleep, total: u32 },   // <- locals that live across the await
    Done,
}
```

The rule, stated once and referred back to all session:

> **Every local that is live across an `.await` becomes a field of the state machine. Every
> `.await` becomes a state.** Locals that do *not* cross an await stay ordinary stack variables
> inside `poll` and cost nothing.

This is why the future's size is what it is: it is the *maximum* set of simultaneously-live
cross-await locals, plus the awaited sub-futures, plus a discriminant.

### 3.2 — Write the machine out by hand (10 min)

**[type this]** — this is `two_step` with the compiler's work done manually:

```rust
enum TwoStepState {
    Start,
    WaitingFirst(Pin<Box<tokio::time::Sleep>>, u32),
    Done,
}

struct TwoStep {
    state: TwoStepState,
}

impl Future for TwoStep {
    type Output = u32;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        loop {
            match &mut this.state {
                TwoStepState::Start => {
                    println!("  state=Start -> compute, then wait");
                    let total = 40;
                    this.state =
                        TwoStepState::WaitingFirst(Box::pin(sleep(Duration::from_millis(20))), total);
                    // loop round: fall straight into the next state
                }
                TwoStepState::WaitingFirst(sleeping, total) => {
                    println!("  state=WaitingFirst -> poll inner sleep");
                    let total = *total;
                    match sleeping.as_mut().poll(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(()) => {
                            this.state = TwoStepState::Done;
                            return Poll::Ready(total + 2);
                        }
                    }
                }
                TwoStepState::Done => panic!("polled after completion"),
            }
        }
    }
}
```

**[expected]** for `TwoStep { state: TwoStepState::Start }.await`:

```
  state=Start -> compute, then wait
  state=WaitingFirst -> poll inner sleep
  state=WaitingFirst -> poll inner sleep
  answer=42
```

### 3.3 — What to point out in the output (2 min)

- **`WaitingFirst` is polled twice.** First poll registers the timer with the driver and returns
  `Pending`. The timer fires, wakes the task, tokio polls the whole future again from the top, the
  `match` lands back in `WaitingFirst`, and this time the inner `Sleep` is `Ready`. **A future's
  `poll` is called repeatedly and must always be able to resume from its recorded state** — there is
  no "continue where the CPU left off".
- **`total` is carried in the enum variant.** It was a local in the `async fn`. It crossed an await,
  so it became a field. Point back at the rule from 3.1.
- **`Done` panics on re-poll.** So does a real `async fn` future ("`async fn` resumed after
  completion"). Futures are not fused; `.await` never polls twice after `Ready`, which is why you
  rarely see it. `ActionFuture` in §5 has the same property and the design doc says so explicitly.
- **The inner `Sleep` had to be `Box::pin`ned.** Ask why, and let it sit. It is the bridge to §4:
  `Sleep::poll` needs `Pin<&mut Sleep>`, and the only way to get one from a field is either to heap-
  pin it (what we did, one allocation, honest but wasteful) or to *project* the outer pin into the
  field (what the compiler does, free, and what makes the machine self-referential).

Real desugaring does **not** box. It stores `Sleep` inline and projects. Which is exactly the
situation the next section is about.

---

## 4. Why the state machine needs `Pin` (15 min)

### 4.1 — The problem, in five lines of async (3 min)

**[type this]**

```rust
async fn borrows_across_await() {
    let owned = String::from("hello");
    let borrowed: &str = &owned;      // a reference into this future's OWN memory
    yield_now().await;
    println!("  after await, borrowed={borrowed}, len={}", owned.len());
}
```

Both `owned` and `borrowed` are live across the `.await`, so both become fields. Draw it:

```
   struct BorrowsAcrossAwait {
       owned:    String,          // at offset 0
       borrowed: *const str,   ───┐  points at offset 0
       state:    u8,              │
   }                          ◄───┘   ... of ITSELF
```

`size_of_val` says 64 bytes for that future on this machine. It contains a pointer to one of its own
fields. **Move those 64 bytes to a different address and the pointer still aims at the old one.**

State the invariant that follows:

> Once a self-referential future has been polled even once, it must never move again.

The compiler cannot express that with lifetimes — the borrow is inside a single value, and the
struct is generated, not written. So the guarantee is moved into the *type of the `poll` receiver*.

### 4.2 — Make the breakage visible (5 min)

Futures are opaque, so demonstrate with a struct you can print. **[type this]**

```rust
use std::marker::PhantomPinned;

struct SelfRef {
    value: u64,
    pointer: *const u64,
    _pin: PhantomPinned,      // opts the type OUT of Unpin
}

impl SelfRef {
    fn new(value: u64) -> Self {
        SelfRef { value, pointer: std::ptr::null(), _pin: PhantomPinned }
    }

    fn init(&mut self) {
        self.pointer = &raw const self.value;   // point at my own field
    }

    fn points_at_self(&self) -> bool {
        std::ptr::eq(self.pointer, &raw const self.value)
    }
}
```

```rust
let mut a = SelfRef::new(1);
a.init();
println!("  before move: pointer aims at own field? {}", a.points_at_self());
let b = a;                       // a plain move. memcpy. no destructor, no fixup.
println!("  after  move: pointer aims at own field? {}", b.points_at_self());
```

**[expected]**

```
  before move: pointer aims at own field? true
  after  move: pointer aims at own field? false
```

**This is the whole reason `Pin` exists.** Two sentences to say here:

- Rust moves are `memcpy` of the bytes and nothing else. There is no move constructor, no relocation
  hook, no way for a type to say "fix up my interior pointers".
- Note that dereferencing `b.pointer` after the move would probably still *print* `1` — the old
  stack slot has not been overwritten yet. That is the worst possible outcome: silent, intermittent
  UB that survives testing. **Do not demo the dereference**; demo the boolean.

### 4.3 — What `Pin` actually is (4 min)

Get this on the board, because everyone mis-models it at first:

> `Pin<P>` is a **wrapper around a pointer** `P` that refuses to hand out a `&mut` to its target,
> unless the target is `Unpin`. That is all. It has no runtime representation and pins nothing by
> itself — it is a *promise, enforced by the absence of an API*, that the pointee will not move
> before it is dropped.

| Myth | Reality |
|---|---|
| "`Pin` pins memory to an address" | It removes the only safe way to move the value: `&mut T`. Without `&mut T` there is no `mem::swap`, no `mem::replace`, no assignment. |
| "`Pin<T>`" | There is no such thing. It is always `Pin<&mut T>`, `Pin<Box<T>>` — a pinned *pointer*. |
| "`Pin` costs something" | Zero. It is a newtype over the pointer, erased at codegen. |
| "I need `Pin` for my types" | Almost certainly not. Everything you write by hand is `Unpin` unless it contains `PhantomPinned` or a coroutine, and `Pin<&mut T>` where `T: Unpin` is just a `&mut T` with extra syntax. |

**`Unpin` is the escape hatch, and it is an auto trait.** `T: Unpin` means "moving this is fine even
after pinning" — true for `u64`, `String`, `Vec<T>`, and every struct you have written so far. It is
*not* true for `async fn` futures. So:

```rust
let mut f = task();
let p = std::pin::Pin::new(&mut f);   // Pin::new requires Unpin
```

**[expected]**

```
error[E0277]: `{async fn body of task()}` cannot be unpinned
    --> app/demo/examples/training_s2.rs:6:32
     |
   1 | async fn task() -> u32 { 1 }
     |                    --- within this `impl Future<Output = u32>`
...
   6 |     let p = std::pin::Pin::new(&mut f);
     |             ------------------ ^^^^^^ within `impl Future<Output = u32>`, the trait `Unpin`
     |                                       is not implemented for `{async fn body of task()}`
     |
     = note: consider using the `pin!` macro
             consider using `Box::pin` if you need to access the pinned value outside of the current scope
```

Read the note out loud — the compiler is telling you the two ways to pin, which is the next slide.

And the mirror image, on a `!Unpin` value:

```rust
let pa = pin!(SelfRef::new(1));
let pb = pin!(SelfRef::new(2));
std::mem::swap(pa.get_mut(), pb.get_mut());
```

**[expected]**

```
error[E0277]: `PhantomPinned` cannot be unpinned
    --> app/demo/examples/training_s2.rs:16:23
     |
  16 |     std::mem::swap(pa.get_mut(), pb.get_mut());
     |                       ^^^^^^^ within `SelfRef`, the trait `Unpin` is not implemented
     |                               for `PhantomPinned`
...
note: required by a bound in `Pin::<&'a mut T>::get_mut`
     |
1598 |         T: Unpin,
     |            ^^^^^ required by this bound in `Pin::<&mut T>::get_mut`
```

`get_mut` is gated on `Unpin`; `mem::swap` needs `&mut`; therefore the value cannot be swapped out
from under its own interior pointer. **That single bound is the entire safety argument.**

### 4.4 — The three ways to get a `Pin` (3 min)

| Form | Where the value lives | Cost | Use when |
|---|---|---|---|
| `std::pin::pin!(value)` | current stack frame | free | you poll it in this scope — this is what `.await` does for you |
| `Box::pin(value)` | heap | one allocation | the future must outlive the frame, or be stored in a collection, or be type-erased as `Pin<Box<dyn Future>>` |
| `Pin::new(&mut value)` | anywhere | free | only when `T: Unpin` |

Then close the loop:

```rust
let mut pinned = pin!(SelfRef::new(2));
unsafe { pinned.as_mut().get_unchecked_mut() }.init();
println!("  pinned: pointer aims at own field? {}", pinned.points_at_self());
```

**[expected]**

```
  pinned: pointer aims at own field? true
```

...and there is now no safe way to invalidate it. `pinned` is `Pin<&mut SelfRef>`, `SelfRef` is not
`Unpin`, so `get_mut` is gone, so `swap`/`replace`/assignment are gone. The pointer stays true until
`drop`.

**Where does `.await` do this?** In the desugaring. `expr.await` pins `expr` in the enclosing
coroutine's frame and polls it there. You have been pinning futures all along; today is just the
first time you had to type it.

---

## 5. The framework exhibit: `ActionFuture` (10 min)

Everything so far was scaffolding for this. Open
[`lib/framework/src/log.rs:86`](../../lib/framework/src/log.rs) alongside
[`doc/action_future_design.md`](../action_future_design.md).

### 5.1 — What it is (3 min)

`log::action(kind, ref_ids, task)` wraps any future so that an `Action` (id, kind, context, stats,
log buffer) is a task-local for that future's lifetime, and is emitted to the appender the moment
the future resolves. It is the framework's centrepiece, and it is a **hand-written `Future`**:

```rust
pin_project! {
    /// Hand written so the task is stored exactly once.
    pub struct ActionFuture<F> {
        #[pin]
        inner: TaskLocalFuture<RefCell<Action>, F>,
    }
}

#[inline]
pub fn action<F: Future>(kind: &'static str, ref_ids: Option<Vec<String>>, task: F) -> ActionFuture<F> {
    let now = DateTime::now();
    let id = id_generator::next_id(now.unix_timestamp_millis());
    let action = Action::new(id, kind, ref_ids, now);
    ActionFuture { inner: CURRENT_ACTION.scope(RefCell::new(action), task) }
}

impl<F, R> Future for ActionFuture<F>
where
    F: Future<Output = Result<R, Exception>>,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        let result = ready!(this.inner.as_mut().poll(cx));

        let mut current_action =
            this.inner.take_value().map(RefCell::into_inner).expect("current action must be within the scope");

        if let Err(e) = &result {
            current_action.log_exception(e);
        }
        current_action.finish();

        if let Some(sender) = SENDER.get() {
            let _result = sender.send(Message::Action(current_action.into()));
        }

        Poll::Ready(result)
    }
}
```

Read it against §2 and §3 and every line is now familiar:

- `pub fn action`, **not** `pub async fn action`. It builds a value and returns it; nothing runs.
- `ready!(..)` is `match { Pending => return Pending, Ready(v) => v }` — the idiom for "delegate to
  the inner future, and if it is not done, neither am I".
- Everything after `ready!` runs **exactly once**, on the poll that resolves. That is the emit path
  for every action record in the system.
- `#[pin]` + `self.project()` is **pin projection**: turning `Pin<&mut ActionFuture<F>>` into
  `Pin<&mut TaskLocalFuture<..>>` for the field. This is the thing §3.2 dodged with `Box::pin`.
  `pin_project_lite` generates the `unsafe` so nobody has to review it by hand.

### 5.2 — Why it is hand-written (4 min)

The obvious implementation is an `async fn`, and it costs 3x the size of the task it wraps. The
mechanism, from the design doc:

> rustc keeps **parameters and upvars in the coroutine layout prefix — always live, never overlapped
> with variant fields**. Only ordinary locals get liveness-based slot reuse.

So an `async fn log::action(.., task: F)` held `task` three times over:

1. in the outer `async fn`'s prefix, as a parameter;
2. in the inner `CURRENT_ACTION.scope(.., async move { .. })` block's prefix, as an upvar;
3. in `__awaitee`, the `into_future(task)` temporary held across the yield.

Every action future contains its caller's future, so this is a **multiplier**, not an overhead.
`clippy::large_futures` denies futures over 16 KB and `.cargo/config.toml` sets `warnings = "deny"`;
a 6 KB jetstream setup task became 18,032 B and failed the build.

### 5.3 — Measure it live (3 min)

**[type this]** — this reproduces the multiplier in 20 lines:

```rust
async fn wrapper_1<F: Future>(task: F) -> F::Output {
    task.await
}

async fn wrapper_2<F: Future>(task: F) -> F::Output {
    async move { task.await }.await
}

async fn big_task() -> u32 {
    let buffer = [0_u8; 1024];
    sleep(Duration::from_millis(1)).await;
    buffer[0] as u32
}

println!("task      = {}", size_of_val(&big_task()));
println!("1 layer   = {}", size_of_val(&wrapper_1(big_task())));
println!("2 layers  = {}", size_of_val(&wrapper_2(big_task())));
```

**[expected]**

```
task      = 1144
1 layer   = 2296
2 layers  = 3448
```

1x, 2x, 3x — plus 8 bytes of discriminant per layer. Then show that a hand-written passthrough is
free:

```rust
struct Passthrough<F> { inner: F }

impl<F: Future> Future for Passthrough<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        unsafe { self.map_unchecked_mut(|s| &mut s.inner) }.poll(cx)
    }
}
```

```
hand written Future = 1144
```

**The caller-side rule falls straight out of this**, and it is the one thing from §5 that
participants must remember when they write handlers in Phase 3:

> **Never wrap `log::action` in an `async fn`.** A function whose only job is to `.await` a
> `log::action(..)` must be a plain `fn` returning `impl Future`, or it re-adds the 2x on top.

The design doc's "three things that are not mechanical" (counter guards must move *inside* the async
block; `async` becomes `async move`; the prologue becomes eager) are worth reading aloud if you have
the minute — they are all consequences of §2.1, laziness, biting in production.

---

## 6. Lab (15 min, pairs)

All of it in `app/demo/examples/`, run with `cargo run -p demo --example <name>`.

### Exercise A — read four errors (5 min)

Appendix C has four snippets. For each: predict the error *before* compiling, then compile. Write
down the error code and the one-sentence rule it enforces. Answer key in Appendix C.

### Exercise B — cancellation is a drop (5 min)

Write a future that holds a `Drop` guard, and cancel it:

```rust
struct Guard(&'static str);

impl Drop for Guard {
    fn drop(&mut self) {
        println!("  Guard({}) dropped", self.0);
    }
}

async fn slow() {
    let _guard = Guard("slow");
    println!("  slow: started");
    sleep(Duration::from_secs(10)).await;
    println!("  slow: finished (never printed)");
}

let result = timeout(Duration::from_millis(100), slow()).await;
println!("  timeout result is_err={}", result.is_err());
```

**[expected]**

```
  slow: started
  Guard(slow) dropped
  timeout result is_err=true, elapsed=102.213709ms
```

Answer in your pair: **where exactly did `slow` stop?** (At the `.await`. Nowhere else. A future can
only be cancelled at a suspension point, because that is the only moment control is outside `poll`.)
Then the follow-up that matters for Phase 3: *if that had been a database transaction between two
awaits, what state is the database in?* This is why `ActionFuture` deliberately does not emit an
action for a cancelled task — "a dropped, incomplete action is still not emitted".

### Exercise C — write a wrapper future (5 min, stretch)

Write `Timed<F>`: wraps a future, prints how long it took, adds **no** async layer. Requirements:

- `fn timed(name: &'static str, inner: F) -> Timed<F>` — a plain `fn`.
- Start the clock on **first poll**, not at construction. (Why? Exercise B's `_guard` bug in the
  design doc is the same mistake.)
- `size_of_val` of `Timed<F>` must be `size_of::<F>()` plus only the fields you added.

Reference solution in Appendix D. This is `ActionFuture` with the interesting parts removed, and
writing it once is the difference between reading `log.rs` and understanding it.

**Homework (carried to the next session):** re-read
[`doc/action_future_design.md`](../action_future_design.md) §"Caller-side rule" and find, in
`lib/framework_nats/src/consumer.rs` or `lib/framework_kafka/src/consumer.rs`, one `Box::pin` that is
**not** a size workaround. Be ready to say what boundary it is erasing.

---

## 7. Wrap (5 min)

### The five sentences

1. An `async fn` returns an inert state machine; nothing runs until something polls it.
2. `poll` returns `Ready` or `Pending`, and `Pending` is a promise that the `Waker` will be called.
3. Locals that live across an `.await` become fields of the machine; that is where a future's size
   comes from.
4. Those fields can point at each other, so the machine must not move after its first poll — which
   is enforced by `poll` taking `Pin<&mut Self>` and `Pin` withholding `&mut`.
5. Because a wrapper future stores its inner future in a prefix slot that is never reused, wrapping
   with `async fn` multiplies size — which is why the framework's `ActionFuture` is hand-written.

### Pitfalls to state explicitly

- **Blocking in async.** No `std::thread::sleep`, no blocking file I/O, no CPU-bound loops on a
  runtime thread. A worker thread that is inside `poll` cannot poll anything else; block one and you
  have stalled every future scheduled on it. Use `tokio::time::sleep`, tokio's I/O, or
  `spawn_blocking`.
- **Holding a `std::sync::MutexGuard` across `.await`.** This one does not compile, and the error is
  worth reading now even though `Send` is Session 7:

  ```
  error: future cannot be sent between threads safely
      = help: within `{async block@...}`, the trait `Send` is not implemented for
              `std::sync::MutexGuard<'_, u32>`
  note: future is not `Send` as this value is used across an await
      |
    8 |         let mut guard = lock.lock().unwrap();
      |             --------- has type `std::sync::MutexGuard<'_, u32>` which is not `Send`
    9 |         yield_now().await;
      |                     ^^^^^ await occurs here, with `mut guard` maybe used later
  ```

  Say only this much today: the guard became a *field* of the future (§3.1's rule), and `tokio::spawn`
  requires the future to be `Send`. The fix is to end the borrow before the await. Park the rest.
- **Assuming an async block starts on creation.** §2.1. It bites hardest with metrics guards and
  timers, which then measure the wrong thing.
- **Bare `tokio::spawn` for app work.** Use `spawn_action!` — see
  [`lib/framework/src/task.rs`](../../lib/framework/src/task.rs). It registers with the `TaskTracker`
  so shutdown can wait for it, and links the child action to the parent by `ref_id`. Session 8.
- **Deep `async fn` wrapper chains.** Every layer that owns a future adds a copy of it. Watch
  `large_futures`.

### Parked list to carry forward

`Send`/`Sync`, `Arc`, `Mutex`, `&'static` (Session 7) · `CancellationToken`, `TaskTracker`, graceful
shutdown, task-locals as an API (Session 8) · what `CURRENT_ACTION` is actually *for* (Session 10).

---

## Appendix A — Cheat sheet

| You want to… | Write |
|---|---|
| Run a future to completion | `future.await` inside an `async` context |
| Start a runtime | `#[tokio::main] async fn main()` |
| Two futures concurrently, one task | `tokio::join!(a, b)` |
| First one to finish wins, cancel the rest | `tokio::select! { .. }` |
| A deadline | `tokio::time::timeout(dur, future).await` |
| Background work, own task | `spawn_action!("name", async { .. })` — not bare `tokio::spawn` |
| Yield to the scheduler | `tokio::task::yield_now().await` |
| Pin on the stack | `let f = std::pin::pin!(future);` |
| Pin on the heap / erase the type | `Box::pin(future)` → `Pin<Box<dyn Future<Output = T> + Send>>` |
| Delegate to an inner future | `let v = ready!(inner.poll(cx));` |
| Project a pin into a field | `pin_project!` + `#[pin]` + `self.project()` |
| Measure a future's size | `size_of_val(&make_future())` |
| Wrap a future without paying 2x | a plain `fn` returning a hand-written `Future` |

**Error decoder:**

| Symptom | Means | Usual fix |
|---|---|---|
| `E0277: cannot be unpinned` on `Pin::new` | the value is `!Unpin` (a future, or has `PhantomPinned`) | `pin!(..)` or `Box::pin(..)` |
| `E0277: cannot be unpinned` on `get_mut` | you are asking for `&mut T` out of a pin | project into the field instead, or reconsider |
| `future cannot be sent between threads safely` | a `!Send` value is live across an `.await` | shorten the borrow so it ends before the await |
| `` `async fn` resumed after completion `` | polled after `Ready` | you are driving `poll` by hand; stop at `Ready` |
| `large future` (clippy) | over `future-size-threshold` at an `.await` | remove an `async fn` wrapper layer; see the design doc |
| future built but never awaited | laziness | `.await` it, or `spawn_action!` it |

---

## Appendix B — Questions this group will ask

**"Is `.await` a thread switch?"**
No. It is a `return` out of `poll`, back to the runtime's loop, on the same thread. The task may be
resumed on a *different* worker thread later (tokio's scheduler steals work), which is why
`tokio::spawn` requires `Send` — but the await itself parks nothing.

**"How many threads does tokio use?"**
By default, one worker per CPU core, plus a blocking pool for `spawn_blocking`. Thousands of tasks
share them. That is the whole pitch: task count is decoupled from thread count.

**"Then why is `#[tokio::main]` needed? Why isn't the runtime built in?"**
Because Rust has no runtime. `Future` and `.await` are language/std; polling them is a library's
job. Embedded and kernel code use different executors entirely.

**"Coming from Java — is this Project Loom?"**
Similar goal, opposite mechanism. Loom keeps the blocking programming model and makes the *stack*
cheap (virtual threads with growable stacks). Rust keeps real stacks and makes the *suspended state*
a compiler-generated struct with no stack at all. The Rust version is smaller and needs no runtime
support; the cost is that it colours functions (`async fn` vs `fn`) and forces `Pin` on you.

**"Coming from TS — my promise already ran. What breaks if I assume that here?"**
Building a future in one place and awaiting it much later means its prologue runs late, not early.
The design doc's third "not mechanical" item is exactly this bug in production: moving work out of
an `async fn` made `DateTime::now()` eager and shifted the action's start stamp by one scheduling
hop.

**"Do I ever have to write `Pin` myself?"**
Only when you hand-write a `Future` (or a `Stream`). In application code — handlers, jobs, services —
you will write `async fn` and `.await` and never type `Pin`. This session exists so that when you
read `log.rs`, or a `large_futures` error, or a `!Unpin` message, none of it is a mystery.

**"Is `unsafe` required to write a `Future`?"**
Only for pin projection, and only if you do it by hand. `pin_project_lite` generates it. If your
future has no future-typed fields (`Countdown` in §2.3), there is no projection and no `unsafe`.

**"Why `RefCell<Action>` and not `Mutex`? Isn't this concurrent?"**
The action is a *task-local*: one task, one action, no sharing. `RefCell` is the single-threaded
interior-mutability tool and it is the right one. There is a real hazard — the comment above
`macro_rules! log` warns never to call `log!` inside a `Display` impl you then pass to `log!`,
because the `RefCell` gets borrowed twice and panics. Session 10.

**"What happens if I `.await` in a loop over a `Vec` of futures?"**
Sequential. You built N state machines and are polling them one at a time. `join_all` or a `JoinSet`
is what you meant.

**"Can I just `Box::pin` everything and stop worrying?"**
You can, and it costs an allocation and a pointer indirection per poll. The design doc rejected
exactly that (`Box::pin(log::action(..))` at every call site) for those reasons plus "per-call-site,
easy to forget". Box when you need type erasure or heap lifetime — not to silence `Pin`.

---

## Appendix C — Exercise A snippets

```rust
// --- 1 ---
use std::time::Duration;
use tokio::time::sleep;

async fn fetch() -> u32 {
    sleep(Duration::from_millis(10)).await;
    1
}

#[tokio::main]
async fn main() {
    let value = fetch();
    println!("{}", value + 1);
}

// --- 2 ---
async fn task() -> u32 { 1 }

#[tokio::main]
async fn main() {
    let mut f = task();
    let p = std::pin::Pin::new(&mut f);
    println!("{}", p.await);
}

// --- 3 ---
use std::sync::Mutex;
use tokio::task::yield_now;

#[tokio::main]
async fn main() {
    let lock = Mutex::new(0_u32);
    tokio::spawn(async move {
        let mut guard = lock.lock().unwrap();
        yield_now().await;
        *guard += 1;
    })
    .await
    .unwrap();
}

// --- 4 ---
use std::marker::PhantomPinned;
use std::pin::pin;

struct SelfRef { value: u64, _pin: PhantomPinned }

#[tokio::main]
async fn main() {
    let a = SelfRef { value: 1, _pin: PhantomPinned };
    let b = SelfRef { value: 2, _pin: PhantomPinned };
    let pa = pin!(a);
    let pb = pin!(b);
    std::mem::swap(pa.get_mut(), pb.get_mut());
}
```

**Answer key**

| # | Error | Rule enforced | Fix |
|---|---|---|---|
| 1 | `E0369: cannot add {integer} to impl Future<Output = u32>` | a future is a *value*, not its output; calling an `async fn` runs nothing | `let value = fetch().await;` |
| 2 | `E0277: {async fn body of task()} cannot be unpinned` | `Pin::new` is safe only for `Unpin`; `async fn` futures are self-referential | `let f = std::pin::pin!(task());` or `Box::pin` |
| 3 | `future cannot be sent between threads safely` | a local live across `.await` becomes a field; `MutexGuard` is `!Send` and `tokio::spawn` needs `Send` | end the borrow before the await: `{ *lock.lock().unwrap() += 1; }` then await |
| 4 | `E0277: PhantomPinned cannot be unpinned` | `Pin::get_mut` is gated on `Unpin`, which is what makes moving impossible | there is no fix — that is the point; the type is pinned by design |

Snippet 3 is the one to spend time on: it is the only one whose error message names the *mechanism*
("this value is used across an await") rather than the symptom. It is also the error people will
actually hit in Phase 3.

---

## Appendix D — Reference solutions

### Exercise C — `Timed<F>`

```rust
use std::future::Future;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::task::ready;
use std::time::Duration;
use std::time::Instant;

use tokio::time::sleep;

/// Wraps a future and prints how long it took, without adding an async layer.
struct Timed<F> {
    name: &'static str,
    started: Option<Instant>,
    inner: F,
}

fn timed<F: Future>(name: &'static str, inner: F) -> Timed<F> {
    Timed { name, started: None, inner }
}

impl<F: Future> Future for Timed<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: `inner` is structurally pinned - it is never moved out of `self`, and no other
        // method hands out a `&mut F`. `name` and `started` are `Unpin`, so touching them is fine.
        let this = unsafe { self.get_unchecked_mut() };
        let started = *this.started.get_or_insert_with(Instant::now);
        let inner = unsafe { Pin::new_unchecked(&mut this.inner) };

        let output = ready!(inner.poll(cx));
        println!("  [{}] elapsed={:?}", this.name, started.elapsed());
        Poll::Ready(output)
    }
}

async fn work() -> u32 {
    sleep(Duration::from_millis(30)).await;
    42
}

#[tokio::main]
async fn main() {
    let value = timed("work", work()).await;
    println!("  value={value}");
    println!("  size_of task  = {}", size_of_val(&work()));
    println!("  size_of Timed = {}", size_of_val(&timed("work", work())));
}
```

**[expected]**

```
  [work] elapsed=32.121125ms
  value=42
  size_of task  = 120
  size_of Timed = 152
```

32 bytes of overhead: a `&'static str` (16) and an `Option<Instant>` (16). No multiplier — the task
is stored once. Write the same wrapper as an `async fn timed(name, inner) { .. inner.await .. }` and
the same measurement reports **296** — the task stored twice, once as a parameter in the prefix and
once as the awaitee.

Two things to review in the pairs' solutions:

1. **`started` is `Option<Instant>`, set on first poll.** A `Timed` that records `Instant::now()` in
   `timed()` measures from *construction*, which for a spawned task includes queue time. This is the
   same trap as the counter guard in `doc/action_future_design.md`: work placed in the constructing
   `fn` runs at call time, not at poll time.
2. **The two `unsafe` blocks are the pin projection.** `get_unchecked_mut` is sound only because
   nothing in this type ever moves `inner` or leaks a `&mut F`. The framework does not write this by
   hand — it uses `pin_project!`:

   ```rust
   pin_project! {
       struct Timed<F> {
           name: &'static str,
           started: Option<Instant>,
           #[pin]
           inner: F,
       }
   }
   ```

   and then `let this = self.project();` gives `this.inner: Pin<&mut F>` and `this.started: &mut
   Option<Instant>` with no `unsafe` in your file. That is exactly the shape of `ActionFuture`.
   `pin-project-lite` is a dependency of `lib/framework`, not of `app/demo`, which is why the lab
   version is hand-rolled.

### Exercise B — the full cancellation example

```rust
use std::time::Duration;
use std::time::Instant;

use tokio::time::sleep;
use tokio::time::timeout;

struct Guard(&'static str);

impl Drop for Guard {
    fn drop(&mut self) {
        println!("  Guard({}) dropped", self.0);
    }
}

async fn slow() {
    let _guard = Guard("slow");
    println!("  slow: started");
    sleep(Duration::from_secs(10)).await;
    println!("  slow: finished (never printed)");
}

#[tokio::main]
async fn main() {
    let started = Instant::now();
    let result = timeout(Duration::from_millis(100), slow()).await;
    println!("  timeout result is_err={}, elapsed={:?}", result.is_err(), started.elapsed());

    tokio::select! {
        () = sleep(Duration::from_millis(10)) => println!("  fast branch won"),
        () = sleep(Duration::from_millis(500)) => println!("  slow branch won"),
    }
}
```

**[expected]**

```
  slow: started
  Guard(slow) dropped
  timeout result is_err=true, elapsed=102.213709ms
  fast branch won
```

The `select!` at the end makes the same point from the other side: the losing branch's future is
dropped where it stood. Anything it was holding — a connection, a permit, a half-written batch — is
released by `Drop`, at that exact `.await`. This is the mechanism behind `system.rs`'s appender drain
loop and every `CancellationToken` in the codebase, which is Session 8.

---

**Next:** Session 7 — Shared state: `Send`, `Sync`, `Arc`, `Mutex`, `&'static`.
