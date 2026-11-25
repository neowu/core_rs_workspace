//! Per-action stats taken around every poll of an action: heap accounting (`alloc_count`,
//! `alloc_bytes`, the counting `#[global_allocator]` lives here) and poll time (`poll_elapsed`,
//! `poll_count`).
//!
//! Design, and what the numbers do and do not include: `spec/action_alloc_stats.md`.

use std::alloc::GlobalAlloc;
use std::alloc::Layout;
use std::alloc::System;
use std::cell::Cell;
use std::sync::OnceLock;
use std::time::Instant;

use crate::log::Severity;
use crate::log::action::Action;
use crate::metrics::collector::container_mem_max;

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

// `(max memory, threshold)`, none when max memory is not available
static HIGH_MEM_ALLOC: OnceLock<Option<(u64, u64)>> = OnceLock::new();

fn high_mem_alloc() -> Option<(u64, u64)> {
    *HIGH_MEM_ALLOC.get_or_init(|| container_mem_max().map(|max| (max, max / 2)))
}

/// Taken as a poll starts, the deltas up to `PollStats::record` are that poll's own work.
pub(crate) struct PollStart {
    time: Instant,
    allocs: (u64, u64),
}

impl PollStart {
    #[inline]
    pub(crate) fn now() -> PollStart {
        PollStart { time: Instant::now(), allocs: counters() }
    }
}

/// What one action cost, accumulated across its polls and written into its stats on finish.
pub(crate) struct PollStats {
    pub(crate) count: u64,
    // nanos spent inside inner poll, i.e. holding the worker thread; waiting is not counted
    pub(crate) elapsed: u64,
    // end of the last poll, the action has been waiting since
    pub(crate) last_polled: Option<Instant>,
    pub(crate) alloc_count: u64,
    pub(crate) alloc_bytes: u64,
}

impl PollStats {
    pub(crate) const fn new() -> PollStats {
        PollStats { count: 0, elapsed: 0, last_polled: None, alloc_count: 0, alloc_bytes: 0 }
    }

    #[inline]
    pub(crate) fn record(&mut self, start: &PollStart) {
        let (allocs, bytes) = counters();
        let end = Instant::now();

        self.count += 1;
        self.elapsed += end.duration_since(start.time).as_nanos() as u64;
        self.last_polled = Some(end);
        self.alloc_count += allocs.wrapping_sub(start.allocs.0);
        self.alloc_bytes += bytes.wrapping_sub(start.allocs.1);
    }
}

pub(crate) fn write_poll_stats(action: &mut Action) {
    let PollStats { count, elapsed, alloc_count, alloc_bytes, .. } = action.poll_stats;
    action.add_stat("alloc_count", alloc_count);
    action.add_stat("alloc_bytes", alloc_bytes);
    action.add_stat("poll_elapsed", elapsed);
    action.add_stat("poll_count", count);

    if let Some((max, threshold)) = high_mem_alloc()
        && alloc_bytes > threshold
    {
        action.log(
            Some(Severity::Warn),
            Some("HIGH_MEM_ALLOC"),
            None,
            format_args!(
                "action allocated more than 50% of max memory in total, alloc_bytes={alloc_bytes}, max_memory={max}"
            ),
        );
    }
}

#[cfg(test)]
mod tests {
    use std::hint::black_box;

    use crate::log::action::Action;
    use crate::log::id_generator::next_id;
    use crate::log::stats::PollStart;
    use crate::log::stats::write_poll_stats;
    use crate::time::DateTime;

    const CAPACITY: usize = 8192;

    fn stat(action: &Action, key: &str) -> Option<u64> {
        action.stats.iter().find(|(existing, _)| existing == key).map(|(_, value)| *value)
    }

    #[test]
    fn accumulates_across_polls_and_writes_stats() {
        let mut action = Action::new(next_id(0), "test", None, DateTime::now());
        // two polls of one action, only the last would survive a delta taken behind `ready!`
        for _ in 0..2 {
            let start = PollStart::now();
            drop(black_box(Vec::<u8>::with_capacity(CAPACITY)));
            action.poll_stats.record(&start);
        }
        assert!(action.poll_stats.last_polled.is_some());

        write_poll_stats(&mut action);

        assert_eq!(stat(&action, "poll_count"), Some(2));
        assert!(stat(&action, "poll_elapsed").is_some());
        assert!(stat(&action, "alloc_count").expect("alloc_count") >= 2, "stats={:?}", action.stats);
        let bytes = stat(&action, "alloc_bytes").expect("alloc_bytes");
        assert!(bytes >= 2 * CAPACITY as u64, "stats={:?}", action.stats);
    }

    #[test]
    fn counts_nothing_outside_a_poll() {
        let mut action = Action::new(next_id(0), "test", None, DateTime::now());
        drop(black_box(Vec::<u8>::with_capacity(CAPACITY)));

        write_poll_stats(&mut action);

        assert_eq!(stat(&action, "alloc_count"), Some(0));
        assert_eq!(stat(&action, "alloc_bytes"), Some(0));
    }
}
