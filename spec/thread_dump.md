# Thread dump design

Code: [`lib/framework/src/thread.rs`](../lib/framework/src/thread.rs) ·
endpoint: [`web/diagnostic.rs`](../lib/framework/src/web/diagnostic.rs) · siblings:
[`action_inflight.md`](action_inflight.md), [`http_server.md`](http_server.md)

`GET /_sys/thread` dumps the stack of every OS thread. In an async app a waiting action is on no
thread, so this is not where a slow request shows up; [`/_sys/action`](action_inflight.md) is. The
thread dump answers the other question: what a thread is doing when it should be idle — a worker
stuck in a blocking call, a lock, a busy loop.

```
threads=12

# [thread] id=41, name=tokio-rt-worker, state=S, cpu=1.2s, action=A1ED..
  std::thread::sleep (library/std/src/thread/functions.rs:120)
  demo::web::slow::{closure#0} (app/demo/src/web.rs:42)
  ..

# [thread] id=42, name=tokio-rt-worker, state=S, cpu=830ms
# [thread] id=43, name=tokio-rt-worker, state=S, cpu=790ms
  ..
```

- `action` is the id of the action the thread is polling, the one `/_sys/action` lists as `running`.
- Threads with the same stack share one, idle workers are most of a dump.
- `state` and `cpu` (user + system) are from `/proc`, Linux only.

## Design decisions

### Each thread captures its own stack, in a signal handler

Nothing can walk another thread's stack in process, so the dump signals each thread in turn and the
handler writes raw return addresses into preallocated statics. It does not allocate or lock: the
interrupted thread may hold the allocator's lock. Symbols are resolved afterwards, on the thread
that asked.

Nothing is installed or running until the first dump, so there is no cost when it is not used.

### `SIGURG`

Ignored by default, so a signal arriving with no handler or no dump in progress does nothing, and a
server has no use for tcp urgent data. The handler only answers when it is the thread being asked.

### Symbols by the `backtrace` crate

std can capture and print the current thread's backtrace, not resolve an address taken elsewhere.
The crate's symbol cache is dropped after every dump, the parsed debug info is large.

The release profile keeps line tables, so frames carry `file:line`; without them only names.

## Behaviour and invariants

- One dump at a time. Threads are captured one after another, so the stacks are microseconds apart,
  not one instant.
- A thread that does not answer within 100ms (signal blocked) is listed without a stack.
- Runs on the calling worker, not `spawn_blocking`: it has to work when the blocking pool is stuck.
  The first frames resolved load debug info, so the call can take a second.
- Linux by `/proc/self/task` and `tgkill`, macOS by mach thread ports for local development, other
  platforms return a not supported message.

## Known gaps

- **The thread list is best effort.** Reading `/proc/self/task` is not a snapshot: a thread exiting
  during the read can cut the list short, so a dump may miss live threads. Accepted for a
  troubleshooting tool, take another dump. The unit test only checks the shape of the output.
- **Interrupted syscalls.** `SA_RESTART` resumes most, but some (`epoll_wait`, `nanosleep`) return
  `EINTR` regardless. std and tokio retry; a C library that does not would see a spurious error.
- **Needs a worker to serve it.** With every worker blocked the endpoint cannot answer, a signal
  triggered dump to stderr would cover that.
- **Unwinding in a signal handler is not strictly async signal safe.** Same approach as sampling
  profilers; glibc 2.35+ finds unwind tables without a lock.
