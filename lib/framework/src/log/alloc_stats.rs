//! Opt-in per-action heap accounting, built only under `--features alloc_stats`.
//!
//! Answers "how many allocations does this action cost" per endpoint, from the action record itself,
//! which a cpu profile and a process wide counter can only hint at.
//!
//! Attribution is per **poll**: `ActionFuture` reads the current thread's counters around every
//! `inner.poll` and accumulates the delta. A poll runs start to finish on one thread and two polls
//! never interleave on one thread, so the delta is the action's own work even though the task
//! migrates between workers. What falls outside that window is charged to nobody — connection setup
//! and header parsing happen before the action opens, the `ActionMessage` conversion and the
//! appender happen after the last poll. The number is comparable between actions, not a complete
//! memory bill.
//!
//! Actions do not overlap, by convention: nothing opens a second `log::action` inside one, so a poll
//! window belongs to exactly one action and the counter delta needs no scope stack. Nesting would
//! double count — the inner allocations would land in both records, and the inner finalization would
//! be charged to the outer action. Keeping the read a bare delta, rather than a guard that pauses and
//! restores a parent scope on every poll, is the trade taken for that convention.
//!
//! The counters are only ever read by the thread that wrote them, which is what lets them be a plain
//! non atomic `Cell`: no sharing, no cache line contention, no atomics at all. The process wide
//! variant in `benchmark/http_test_server/src/alloc_stats.rs` needs 64 padded shards of `AtomicU64`
//! for exactly the reason this does not — it sums across threads.
//!
//! Deallocation is deliberately not tracked. A block allocated in an action is routinely freed by
//! another task (the `ActionMessage` is freed by the appender daemon), so per action live bytes
//! would be meaningless and frequently negative, and stats are `u64`. Process rss is
//! `MetricsCollector`'s job.
//!
//! Installing the allocator here means an app cannot enable the feature and silently record zeros.
//! The cost is that the crate graph must then not declare a second `#[global_allocator]`, which
//! `benchmark/http_test_server` does under its own feature of the same name — the two are mutually
//! exclusive.

#[cfg(feature = "alloc_stats")]
pub(crate) use tracking::ActionAllocs;
#[cfg(not(feature = "alloc_stats"))]
pub(crate) use untracked::ActionAllocs;

#[cfg(feature = "alloc_stats")]
mod tracking {
    use std::alloc::GlobalAlloc;
    use std::alloc::Layout;
    use std::alloc::System;
    use std::cell::Cell;

    use crate::log::action::Action;

    thread_local! {
        // `(allocations, bytes allocated)`, const initialised and with no destructor, so reading it
        // neither allocates nor observes a half destroyed thread local
        static COUNTERS: Cell<(u64, u64)> = const { Cell::new((0, 0)) };
    }

    // try_with, because the allocator is still called while thread locals are being destroyed
    fn record(bytes: u64) {
        let _result = COUNTERS.try_with(|counters| {
            let (allocs, total) = counters.get();
            counters.set((allocs.wrapping_add(1), total.wrapping_add(bytes)));
        });
    }

    fn counters() -> (u64, u64) {
        COUNTERS.try_with(Cell::get).unwrap_or((0, 0))
    }

    struct Tracking;

    // realloc and alloc_zeroed delegate to System rather than falling back to the default
    // alloc + copy + dealloc, which would change allocator behaviour instead of only measuring it
    unsafe impl GlobalAlloc for Tracking {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            record(layout.size() as u64);
            unsafe { System.alloc(layout) }
        }

        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            record(layout.size() as u64);
            unsafe { System.alloc_zeroed(layout) }
        }

        // a pure passthrough, deallocation is not tracked, see the module comment
        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            unsafe { System.dealloc(ptr, layout) }
        }

        // only growth counts as bytes allocated, a shrinking realloc allocates nothing
        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            record(new_size.saturating_sub(layout.size()) as u64);
            unsafe { System.realloc(ptr, layout, new_size) }
        }
    }

    #[global_allocator]
    static ALLOCATOR: Tracking = Tracking;

    /// What one action allocated, accumulated across its polls and written into its stats.
    pub(crate) struct ActionAllocs {
        allocs: u64,
        bytes: u64,
    }

    impl ActionAllocs {
        pub(crate) const fn new() -> Self {
            ActionAllocs { allocs: 0, bytes: 0 }
        }

        /// Wraps one poll of the action's task, the counter delta over it is that poll's own work.
        #[inline]
        pub(crate) fn poll<T>(&mut self, poll: impl FnOnce() -> T) -> T {
            let (allocs, bytes) = counters();
            let result = poll();
            let (polled_allocs, polled_bytes) = counters();

            self.allocs += polled_allocs.wrapping_sub(allocs);
            self.bytes += polled_bytes.wrapping_sub(bytes);

            result
        }

        pub(crate) fn write_to(&self, action: &mut Action) {
            action.add_stat("alloc_count", self.allocs);
            action.add_stat("alloc_bytes", self.bytes);
        }
    }
}

#[cfg(not(feature = "alloc_stats"))]
mod untracked {
    use crate::log::action::Action;

    /// Zero sized twin of the tracking accumulator, so `ActionFuture` keeps its size and every call
    /// here compiles away.
    pub(crate) struct ActionAllocs;

    // the signatures mirror the tracking twin, so `&mut self` stays even where the body has no use
    // for it
    #[allow(clippy::needless_pass_by_ref_mut, clippy::unused_self)]
    impl ActionAllocs {
        pub(crate) const fn new() -> Self {
            ActionAllocs
        }

        #[inline]
        pub(crate) fn poll<T>(&mut self, poll: impl FnOnce() -> T) -> T {
            poll()
        }

        #[inline]
        pub(crate) const fn write_to(&self, _action: &mut Action) {}
    }
}

// the cfg(test) has to sit on the outer module for clippy::tests_outside_test_module, so the feature
// split happens one level in
#[cfg(test)]
mod tests {
    #[cfg(feature = "alloc_stats")]
    mod tracking {
        use std::hint::black_box;

        use crate::log::alloc_stats::ActionAllocs;
        use crate::log::action::Action;
        use crate::time::DateTime;

        const CAPACITY: usize = 8192;

        fn stat(action: &Action, key: &str) -> Option<u64> {
            action.stats.iter().find(|(existing, _)| existing == key).map(|(_, value)| *value)
        }

        #[test]
        fn accumulates_across_polls_and_writes_both_stats() {
            let mut allocs = ActionAllocs::new();
            // two calls stand in for two polls of the same action; only the last one would survive
            // if ActionFuture still took its delta behind `ready!`
            for _ in 0..2 {
                allocs.poll(|| drop(black_box(Vec::<u8>::with_capacity(CAPACITY))));
            }

            let mut action = Action::new("id".to_owned(), "test", None, DateTime::now());
            allocs.write_to(&mut action);

            assert!(stat(&action, "alloc_count").expect("alloc_count") >= 2, "stats={:?}", action.stats);
            let bytes = stat(&action, "alloc_bytes").expect("alloc_bytes");
            assert!(bytes >= 2 * CAPACITY as u64, "stats={:?}", action.stats);
        }

        #[test]
        fn counts_nothing_outside_a_poll() {
            let allocs = ActionAllocs::new();
            drop(black_box(Vec::<u8>::with_capacity(CAPACITY)));

            let mut action = Action::new("id".to_owned(), "test", None, DateTime::now());
            allocs.write_to(&mut action);

            assert_eq!(stat(&action, "alloc_count"), Some(0));
            assert_eq!(stat(&action, "alloc_bytes"), Some(0));
        }
    }

    #[cfg(not(feature = "alloc_stats"))]
    mod untracked {
        use crate::log::alloc_stats::ActionAllocs;
        use crate::log::action::Action;
        use crate::time::DateTime;

        #[test]
        fn writes_no_stats() {
            let mut action = Action::new("id".to_owned(), "test", None, DateTime::now());
            ActionAllocs::new().write_to(&mut action);

            // only the reserved `elapsed` slot, no alloc keys at all when the feature is off
            assert_eq!(action.stats.iter().map(|(key, _)| key.as_ref()).collect::<Vec<_>>(), vec!["elapsed"]);
        }
    }
}
