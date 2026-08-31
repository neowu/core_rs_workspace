use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;

/// Tracks max count between collecting.
#[derive(Default)]
pub struct Counter {
    count: AtomicU32,
    max: AtomicU32,
}

pub struct CounterGuard<'a>(&'a Counter);

impl Drop for CounterGuard<'_> {
    fn drop(&mut self) {
        self.0.decrease();
    }
}

impl Counter {
    pub fn increase(&self) -> CounterGuard<'_> {
        let current = self.count.fetch_add(1, Ordering::Relaxed) + 1;
        // only increase() may change max, no need to handle when decrease()
        self.max.fetch_max(current, Ordering::Relaxed);
        CounterGuard(self)
    }

    /// Returns the max since the previous call, and resets it to the current count.
    /// Resets before reading count, so a concurrent increase() either raises max after the
    /// swap (fetch_max only ever raises, so it survives) or shows up in the count we read.
    /// Assumes a single reader.
    pub fn max(&self) -> u32 {
        let max = self.max.swap(0, Ordering::Relaxed);
        let count = self.count.load(Ordering::Relaxed);
        self.max.fetch_max(count, Ordering::Relaxed);
        max
    }

    fn decrease(&self) {
        self.count.fetch_sub(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;

    use super::Counter;

    #[test]
    fn counter_with_reset_max() {
        let counter = Counter::default();
        {
            let _guard_1 = counter.increase();
            let _guard_2 = counter.increase();
        }
        assert_eq!(counter.max(), 2);
        assert_eq!(counter.max(), 0);

        let _guard = counter.increase();
        assert_eq!(counter.max(), 1);
        assert_eq!(counter.max(), 1);
    }

    #[test]
    fn max_resets_to_active_count() {
        let counter = Counter::default();
        let held = counter.increase();
        {
            let _transient = counter.increase();
        }

        // the transient peak is reported, then max drops back to what is still held
        assert_eq!(counter.max(), 2);
        assert_eq!(counter.max(), 1);

        drop(held);
        assert_eq!(counter.max(), 1);
        assert_eq!(counter.max(), 0);
    }

    #[test]
    fn max_keeps_active_count_under_concurrency() {
        let counter = Arc::new(Counter::default());
        // never released, so no sample may drop below it
        let _held = counter.increase();

        let mut handles = Vec::with_capacity(4);
        for _ in 0..4 {
            let counter = Arc::clone(&counter);
            handles.push(thread::spawn(move || {
                for _ in 0..10_000 {
                    let _guard = counter.increase();
                }
            }));
        }

        for _ in 0..1_000 {
            assert!(counter.max() >= 1);
        }

        for handle in handles {
            handle.join().expect("worker must not panic");
        }
        assert!(counter.max() >= 1);
    }
}
