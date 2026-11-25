//! Where an action lives while it runs: the thread's current action, and the registry of actions in
//! flight that `/_sys/action` dumps.
//!
//! Design: `spec/action_inflight.md`.

use std::cell::Cell;
use std::cell::RefCell;
use std::cell::UnsafeCell;
use std::hint;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::ptr;
use std::sync::Mutex;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Instant;
use std::vec;

use crate::log::action::Action;
use crate::log::id_generator::Id;
use crate::log::mask::mask_logs;
use crate::write_str;

const SHARDS: usize = 16;

const IDLE: u8 = 0;
const POLLING: u8 = 1;
const DUMPING: u8 = 2;

static NEXT_SHARD: AtomicUsize = AtomicUsize::new(0);
static REGISTRY: [Shard; SHARDS] = [const { Shard(Mutex::new(Slab { cells: Vec::new(), free: Vec::new() })) }; SHARDS];

thread_local! {
    // the action being polled on this thread, null outside a poll
    static CURRENT: Cell<*const ActionCell> = const { Cell::new(ptr::null()) };
    // a thread registers into its own shard, so the lock is uncontended unless a task migrated
    static SHARD: usize = NEXT_SHARD.fetch_add(1, Ordering::Relaxed) % SHARDS;
}

// own cache line per shard
#[repr(align(64))]
struct Shard(Mutex<Slab>);

struct Slab {
    // null when free
    cells: Vec<*const ActionCell>,
    free: Vec<u32>,
}

// SAFETY: a registered cell stays alive until it unregisters under the shard lock
unsafe impl Send for Slab {}

/// None outside an action.
#[inline]
pub(crate) fn with_current<R>(f: impl FnOnce(&RefCell<Action>) -> R) -> Option<R> {
    let cell = CURRENT.get();
    // SAFETY: points at the cell entered further up this stack, which holds POLLING, the guard resets it on exit
    let action = unsafe { (*cell.as_ref()?.action.get()).as_ref() }?;
    Some(f(action))
}

/// Copies the id of the action being polled on this thread, false outside an action. For the thread
/// dump's signal handler: no allocation, no borrow.
pub(crate) fn copy_current_id(out: &[AtomicU8; Id::LEN]) -> bool {
    let cell = CURRENT.get();
    // SAFETY: same thread as the poll that set it
    let Some(cell) = (unsafe { cell.as_ref() }) else {
        return false;
    };
    for (slot, byte) in out.iter().zip(cell.id.as_bytes()) {
        slot.store(*byte, Ordering::Relaxed);
    }
    true
}

/// An action, owned by `ActionFuture`. Registered by address on the first poll,
/// so it must stay pinned.
pub(crate) struct ActionCell {
    // copied out of the action, never written again, so a dump can read them while the action is being polled
    id: Id,
    kind: &'static str,
    start_time: Instant,
    // IDLE / POLLING / DUMPING, whoever holds it owns `action`
    state: AtomicU8,
    // shard and index in the registry, only touched by the owner
    slot: Cell<Option<(u32, u32)>>,
    // None once finished
    action: UnsafeCell<Option<RefCell<Action>>>,
    _pinned: PhantomPinned,
}

struct EnterGuard<'a> {
    cell: &'a ActionCell,
    previous: *const ActionCell,
}

impl Drop for EnterGuard<'_> {
    fn drop(&mut self) {
        CURRENT.set(self.previous);
        self.cell.state.store(IDLE, Ordering::Release);
    }
}

impl ActionCell {
    pub(crate) const fn new(action: Action) -> ActionCell {
        ActionCell {
            id: action.id,
            kind: action.kind,
            start_time: action.start_time,
            state: AtomicU8::new(IDLE),
            slot: Cell::new(None),
            action: UnsafeCell::new(Some(RefCell::new(action))),
            _pinned: PhantomPinned,
        }
    }

    /// Runs one poll of the action's task, with the action as the thread's current one.
    #[inline]
    pub(crate) fn poll<T>(self: Pin<&ActionCell>, poll: impl FnOnce() -> T) -> T {
        let cell = self.get_ref();
        if cell.slot.get().is_none() {
            cell.register();
        }
        cell.enter(poll)
    }

    /// Runs `f` with the action as the thread's current one.
    #[inline]
    pub(crate) fn enter<T>(&self, f: impl FnOnce() -> T) -> T {
        // only contended while a dump is copying this action
        while self.state.compare_exchange_weak(IDLE, POLLING, Ordering::Acquire, Ordering::Relaxed).is_err() {
            hint::spin_loop();
        }
        let _guard = EnterGuard { cell: self, previous: CURRENT.replace(ptr::from_ref(self)) };
        f()
    }

    /// Takes the action out once its task has completed, it is no longer in flight.
    pub(crate) fn finish(self: Pin<&ActionCell>) -> Action {
        let cell = self.get_ref();
        cell.unregister();
        // SAFETY: unregistered and not being polled, only the owner reaches the action
        let action = unsafe { &mut *cell.action.get() };
        action.take().map(RefCell::into_inner).expect("action must not be finished twice")
    }

    fn register(&self) {
        let shard = SHARD.with(|shard| *shard);
        let mut slab = REGISTRY[shard].0.lock().unwrap();
        let cell = ptr::from_ref(self);
        let index = if let Some(index) = slab.free.pop() {
            slab.cells[index as usize] = cell;
            index
        } else {
            slab.cells.push(cell);
            (slab.cells.len() - 1) as u32
        };
        self.slot.set(Some((shard as u32, index)));
    }

    // waits for a dump reading this cell, it is not read after this returns
    fn unregister(&self) {
        if let Some((shard, index)) = self.slot.take() {
            let mut slab = REGISTRY[shard as usize].0.lock().unwrap();
            slab.cells[index as usize] = ptr::null();
            slab.free.push(index);
        }
    }

    // called under the shard lock
    fn snapshot(&self) -> String {
        let now = Instant::now();
        let elapsed = now.saturating_duration_since(self.start_time);
        let mut out = String::new();
        write_str!(out, "\n# [in-flight] id={}, kind={}, ", self.id, self.kind);
        if self.lock_for_dump() {
            write_str!(out, "state=suspended, elapsed={elapsed:?}\n");
            // SAFETY: DUMPING keeps the poll out, and nothing holds a borrow outside a poll
            if let Some(action) = unsafe { (*self.action.get()).as_ref() } {
                out.push_str(unsafe { &(*action.as_ptr()).logs });
            }
            self.state.store(IDLE, Ordering::Release);
        } else {
            // being polled, the action itself is being written
            write_str!(out, "state=running, elapsed={elapsed:?}\n");
        }
        out
    }

    // a poll is usually over in microseconds, so yield a few times before reporting the action as running
    fn lock_for_dump(&self) -> bool {
        for _ in 0..3 {
            if self.state.compare_exchange(IDLE, DUMPING, Ordering::Acquire, Ordering::Relaxed).is_ok() {
                return true;
            }
            thread::yield_now();
        }
        false
    }
}

impl Drop for ActionCell {
    fn drop(&mut self) {
        self.unregister();
    }
}

/// The actions in flight, oldest first, one chunk of text per action with the trace it has written so far.
///
/// Lazy, so the response is streamed: a chunk is copied when it is pulled, one trace is in memory at a
/// time and a shard is locked for one trace at a time.
pub(crate) struct Dump {
    header: Option<String>,
    entries: vec::IntoIter<Entry>,
}

struct Entry {
    cell: *const ActionCell,
    shard: usize,
    index: usize,
    start_time: Instant,
}

// SAFETY: the pointer is only read under the shard lock, after checking the cell is still registered
unsafe impl Send for Dump {}

/// Lists the actions in flight now, the action calling it is left out.
pub(crate) fn dump() -> Dump {
    let current = CURRENT.get();
    let mut entries = Vec::new();
    for (shard, slab) in REGISTRY.iter().enumerate() {
        let slab = slab.0.lock().unwrap();
        for (index, &cell) in slab.cells.iter().enumerate() {
            // SAFETY: a registered cell is alive, unregistering takes this lock
            if let Some(registered) = unsafe { cell.as_ref() }
                && !ptr::eq(cell, current)
            {
                entries.push(Entry { cell, shard, index, start_time: registered.start_time });
            }
        }
    }
    entries.sort_by_key(|entry| entry.start_time);
    Dump { header: Some(format!("in_flight_actions={}\n", entries.len())), entries: entries.into_iter() }
}

impl Iterator for Dump {
    type Item = String;

    fn next(&mut self) -> Option<String> {
        if let Some(header) = self.header.take() {
            return Some(header);
        }
        for entry in self.entries.by_ref() {
            let Some(shard) = REGISTRY.get(entry.shard) else {
                continue;
            };
            let slab = shard.0.lock().unwrap();
            // finished since it was listed, or its slot went to another action
            if slab.cells.get(entry.index).is_none_or(|cell| !ptr::eq(*cell, entry.cell)) {
                continue;
            }
            // SAFETY: still registered, so alive while the lock is held
            let cell = unsafe { &*entry.cell };
            if cell.start_time != entry.start_time {
                continue;
            }
            let mut out = cell.snapshot();
            drop(slab);
            // the trace is only masked when an action finishes
            mask_logs(&mut out);
            return Some(out);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use std::pin::pin;
    use std::thread;

    use super::ActionCell;
    use super::with_current;
    use crate::log::action::Action;
    use crate::log::id_generator::Id;
    use crate::log::id_generator::next_id;
    use crate::time::DateTime;

    fn dump() -> String {
        super::dump().collect()
    }

    // tests share the registry, each finds its own action by id
    fn action() -> (Id, Action) {
        let id = next_id(0);
        (id, Action::new(id, "test", None, DateTime::now()))
    }

    fn entry(dump: &str, id: Id) -> Option<&str> {
        dump.split("\n\n").find(|entry| entry.starts_with(&format!("# [in-flight] id={id},")))
    }

    #[test]
    fn dump_suspended_action_with_masked_trace() {
        let (id, action) = action();
        let cell = pin!(ActionCell::new(action));
        assert!(entry(&dump(), id).is_none()); // not polled yet

        cell.as_ref().poll(|| {
            crate::log!(r#"body={{"password":"secret"}}"#);
        });

        let dump = dump();
        let entry = entry(&dump, id).expect("action must be in flight");
        assert!(entry.contains("kind=test, state=suspended, elapsed="), "entry={entry}");
        assert!(entry.contains(&format!("# [action] id={id}, kind=test")), "entry={entry}");
        assert!(entry.contains(r#"body={"password":"**masked**"}"#), "entry={entry}");
    }

    #[test]
    fn dump_running_action_without_trace() {
        let (id, action) = action();
        let cell = pin!(ActionCell::new(action));
        let dump = cell.as_ref().poll(|| {
            crate::log!("message");
            // as inside log!, the action is mutably borrowed while the dump reads the entry
            with_current(|action| {
                let _action = action.borrow_mut();
                thread::scope(|scope| scope.spawn(dump).join().expect("dump must not panic"))
            })
            .expect("action must be current")
        });

        let entry = entry(&dump, id).expect("action must be in flight");
        assert!(entry.contains("kind=test, state=running, elapsed="), "entry={entry}");
        assert!(!entry.contains("message"), "entry={entry}");
    }

    #[test]
    fn dump_skips_current_action() {
        let (id, action) = action();
        let cell = pin!(ActionCell::new(action));
        let dump = cell.as_ref().poll(dump);
        assert!(entry(&dump, id).is_none());
    }

    #[test]
    fn finished_or_dropped_action_is_not_in_flight() {
        let (finished_id, finished) = action();
        let (dropped_id, dropped) = action();
        {
            let finished = pin!(ActionCell::new(finished));
            finished.as_ref().poll(|| ());
            let dropped = pin!(ActionCell::new(dropped));
            dropped.as_ref().poll(|| ());
            assert!(entry(&dump(), finished_id).is_some());
            assert!(entry(&dump(), dropped_id).is_some());

            let action = finished.as_ref().finish();
            assert_eq!(action.id, finished_id);
            assert!(entry(&dump(), finished_id).is_none());
        }
        assert!(entry(&dump(), dropped_id).is_none());
    }

    #[test]
    fn dump_skips_action_finished_after_listing() {
        let (id, action) = action();
        let cell = pin!(ActionCell::new(action));
        cell.as_ref().poll(|| ());

        let mut listed = super::dump();
        drop(cell.as_ref().finish());
        assert!(listed.next().is_some_and(|header| header.starts_with("in_flight_actions=")));
        assert!(listed.all(|entry| !entry.contains(id.as_str())));
    }

    #[test]
    fn current_action_is_scoped_to_the_poll() {
        let id = || with_current(|action| action.borrow().id);
        assert_eq!(id(), None);

        let (outer_id, outer) = action();
        let (inner_id, inner) = action();
        let outer = pin!(ActionCell::new(outer));
        let inner = pin!(ActionCell::new(inner));
        outer.as_ref().poll(|| {
            assert_eq!(id(), Some(outer_id));
            inner.as_ref().poll(|| assert_eq!(id(), Some(inner_id)));
            assert_eq!(id(), Some(outer_id));
        });
        assert_eq!(id(), None);
    }
}
