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

    pub fn max(&self) -> u32 {
        self.max.swap(self.count.load(Ordering::Relaxed), Ordering::Relaxed)
    }

    fn decrease(&self) {
        self.count.fetch_sub(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
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
}
