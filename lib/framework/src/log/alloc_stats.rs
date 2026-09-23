//! Per-action heap accounting: `alloc_count` and `alloc_bytes` on every action record. The counting
//! `#[global_allocator]` lives here and `ActionFuture` takes the counter delta around every poll.
//!
//! Design, and what the numbers do and do not include: `spec/action_alloc_stats.md`.

use std::alloc::GlobalAlloc;
use std::alloc::Layout;
use std::alloc::System;
use std::cell::Cell;

use crate::log::action::Action;

thread_local! {
    // `(allocations, bytes allocated)`, const init so reading it never allocates
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

// every method delegates to System, measuring must not change allocator behaviour
unsafe impl GlobalAlloc for Tracking {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record(layout.size() as u64);
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record(layout.size() as u64);
        unsafe { System.alloc_zeroed(layout) }
    }

    // deallocation is not tracked
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
    count: u64,
    bytes: u64,
}

impl ActionAllocs {
    pub(crate) const fn new() -> Self {
        ActionAllocs { count: 0, bytes: 0 }
    }

    /// Wraps one poll, the counter delta over it is that poll's own work.
    #[inline]
    pub(crate) fn poll<T>(&mut self, poll: impl FnOnce() -> T) -> T {
        let (allocs, bytes) = counters();
        let result = poll();
        let (polled_allocs, polled_bytes) = counters();

        self.count += polled_allocs.wrapping_sub(allocs);
        self.bytes += polled_bytes.wrapping_sub(bytes);

        result
    }

    pub(crate) fn write_to(&self, action: &mut Action) {
        action.add_stat("alloc_count", self.count);
        action.add_stat("alloc_bytes", self.bytes);
    }
}

#[cfg(test)]
mod tests {
    use std::hint::black_box;

    use crate::log::action::Action;
    use crate::log::alloc_stats::ActionAllocs;
    use crate::time::DateTime;

    const CAPACITY: usize = 8192;

    fn stat(action: &Action, key: &str) -> Option<u64> {
        action.stats.iter().find(|(existing, _)| existing == key).map(|(_, value)| *value)
    }

    #[test]
    fn accumulates_across_polls_and_writes_both_stats() {
        let mut allocs = ActionAllocs::new();
        // two polls of one action, only the last would survive a delta taken behind `ready!`
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
