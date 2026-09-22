//! Opt-in heap accounting, built only under `--features alloc_stats`.
//!
//! Answers "how many allocations does a request cost", which a cpu profile can only hint at.
//!
//! Counters are **sharded per thread and padded to a cache line**. The obvious version — four
//! shared `AtomicU64` — costs 3.6x cpu per request here, not because atomics are slow but because
//! every allocating thread writes the same cache line. Peak live bytes is deliberately not tracked:
//! it is the one figure that needs a global `fetch_max`, and `run_*.sh` samples rss from outside for
//! free.

use std::alloc::GlobalAlloc;
use std::alloc::Layout;
use std::alloc::System;
use std::cell::Cell;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;

const SHARDS: usize = 64;

#[repr(align(128))]
struct Shard {
    allocs: AtomicU64,
    bytes: AtomicU64,
    // a block is freed by whatever thread drops it, so a shard's live bytes can go negative,
    // only the sum across shards is meaningful
    live: AtomicI64,
}

impl Shard {
    const fn new() -> Self {
        Shard { allocs: AtomicU64::new(0), bytes: AtomicU64::new(0), live: AtomicI64::new(0) }
    }
}

static COUNTERS: [Shard; SHARDS] = [const { Shard::new() }; SHARDS];
static NEXT: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static SHARD: Cell<usize> = const { Cell::new(usize::MAX) };
}

// try_with, because the allocator is still called while thread locals are being destroyed
fn shard() -> &'static Shard {
    SHARD
        .try_with(|cell| {
            let mut index = cell.get();
            if index == usize::MAX {
                index = NEXT.fetch_add(1, Relaxed) % SHARDS;
                cell.set(index);
            }
            &COUNTERS[index]
        })
        .unwrap_or(&COUNTERS[0])
}

pub struct Tracking;

// realloc and alloc_zeroed delegate to System rather than falling back to the default
// alloc + copy + dealloc, which would change allocator behaviour instead of only measuring it
unsafe impl GlobalAlloc for Tracking {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record(layout.size() as i64);
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record(layout.size() as i64);
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        shard().live.fetch_sub(layout.size() as i64, Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let grown = new_size as i64 - layout.size() as i64;
        let shard = shard();
        shard.allocs.fetch_add(1, Relaxed);
        shard.bytes.fetch_add(grown.max(0) as u64, Relaxed);
        shard.live.fetch_add(grown, Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

fn record(size: i64) {
    let shard = shard();
    shard.allocs.fetch_add(1, Relaxed);
    shard.bytes.fetch_add(size as u64, Relaxed);
    shard.live.fetch_add(size, Relaxed);
}

/// `(allocations, bytes allocated, live bytes)`.
pub fn snapshot() -> (u64, u64, i64) {
    COUNTERS.iter().fold((0, 0, 0), |(a, b, l), shard| {
        (a + shard.allocs.load(Relaxed), b + shard.bytes.load(Relaxed), l + shard.live.load(Relaxed))
    })
}
