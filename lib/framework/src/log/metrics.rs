use std::fs;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use chrono::DateTime;
use chrono::Utc;
use tokio::time::sleep;

use crate::log::CONTEXT;
use crate::log::Context;
use crate::log::action::Error;
use crate::log::id_generator;
use crate::log::id_generator::LogId;
use crate::number::parse_u64;

pub struct Metrics {
    pub id: LogId,
    pub date: DateTime<Utc>,
    pub error: Option<Error>,
    pub stats: Vec<(&'static str, u64)>,
    pub info: Vec<(&'static str, String)>,
}

pub struct MetricsCollector {
    previous_cpu_stats: Option<PreviousCpuStats>,
    collectors: Vec<Collector>,
}

type Collector = Box<dyn Fn(&mut Metrics) + Send>;

struct PreviousCpuStats {
    time: Instant,
    cpu_time: u64,
    cpu_max: f64,
}

impl MetricsCollector {
    pub fn new() -> Self {
        let now = Instant::now();
        let previous_cpu_stats = if let Some(cpu_time) = cpu_time()
            && let Some(cpu_max) = cpu_max()
        {
            Some(PreviousCpuStats { time: now, cpu_time, cpu_max })
        } else {
            None
        };
        Self { previous_cpu_stats, collectors: Vec::new() }
    }

    pub fn add(&mut self, collector: impl Fn(&mut Metrics) + Send + 'static) {
        self.collectors.push(Box::new(collector));
    }

    pub fn start_collect_task(self) {
        let mut collector = self;
        if let Some(Context { app, appender }) = CONTEXT.get() {
            tokio::spawn(async move {
                loop {
                    sleep(Duration::from_secs(5)).await;

                    let metrics = collector.collect_metrics();
                    appender.append_metrics(&metrics, app);
                }
            });
        }
    }

    fn collect_metrics(&mut self) -> Metrics {
        let date = Utc::now();
        let mut metrics = Metrics {
            id: id_generator::next_id(date.timestamp_millis()),
            date,
            error: None,
            stats: Vec::new(),
            info: Vec::new(),
        };

        if let Some(cpu_stats) = &mut self.previous_cpu_stats {
            collect_vm_info(&mut metrics, cpu_stats);
        }

        for collector in &self.collectors {
            collector(&mut metrics);
        }

        metrics
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

// collects cpu/memory usage in docker with cgroup v2 (the only supported env)
fn collect_vm_info(metrics: &mut Metrics, previous_cpu_stats: &mut PreviousCpuStats) {
    if let Some(mem_used) = mem_used() {
        metrics.stats.push(("mem_used", mem_used));
    }

    if let Some(mem_max) = mem_max() {
        metrics.stats.push(("mem_max", mem_max));
    }

    let now = Instant::now();
    if let Some(cpu_time) = cpu_time() {
        let wall_time_elapsed = now.duration_since(previous_cpu_stats.time).as_micros() as u64;
        let cpu_time_elapsed = cpu_time.saturating_sub(previous_cpu_stats.cpu_time);
        if let Some(cpu_usage) = cpu_usage(cpu_time_elapsed, wall_time_elapsed, previous_cpu_stats.cpu_max) {
            metrics.stats.push(("cpu_usage", cpu_usage));
        }

        previous_cpu_stats.time = now;
        previous_cpu_stats.cpu_time = cpu_time;
    }
}

fn mem_used() -> Option<u64> {
    let content = fs::read_to_string("/sys/fs/cgroup/memory.current").ok()?;
    parse_u64(content.trim()).ok()
}

fn mem_max() -> Option<u64> {
    let content = fs::read_to_string("/sys/fs/cgroup/memory.max").ok()?;
    let content = content.trim();
    if content != "max" {
        return parse_u64(content).ok();
    }
    unsafe {
        let pages = libc::sysconf(libc::_SC_PHYS_PAGES);
        let page_size = libc::sysconf(libc::_SC_PAGESIZE);
        if pages > 0 && page_size > 0 {
            return Some(pages as u64 * page_size as u64);
        }
    }
    None
}

// only for docker w/ cgroup v2
fn cpu_time() -> Option<u64> {
    let content = fs::read_to_string("/sys/fs/cgroup/cpu.stat").ok()?;
    for line in content.lines() {
        if let Some(value) = line.strip_prefix("usage_usec ") {
            return parse_u64(value.trim()).ok();
        }
    }
    None
}

fn cpu_usage(cpu_time_elapsed: u64, wall_time_elapsed: u64, cpu_max: f64) -> Option<u64> {
    if wall_time_elapsed == 0 {
        return None;
    }
    let cpu_used = cpu_time_elapsed as f64 / wall_time_elapsed as f64;
    let usage = (cpu_used / cpu_max * 100.0).round();
    Some(usage as u64)
}

// percent of cpu quota (cpu.max), 100 = at the limit; percent of raw cores used if no quota set
fn cpu_max() -> Option<f64> {
    let content = fs::read_to_string("/sys/fs/cgroup/cpu.max").ok()?;
    let mut parts = content.split_whitespace();
    let quota = parts.next()?;
    if quota == "max" {
        Some(1.0)
    } else {
        let quota = parse_u64(quota).ok()?;
        let period = parse_u64(parts.next()?).ok()?;
        Some(quota as f64 / period as f64)
    }
}

/// Tracks max count between collecting.
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
    pub const fn new() -> Self {
        Self { count: AtomicU32::new(0), max: AtomicU32::new(0) }
    }

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

impl Default for Counter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use crate::log::metrics::Counter;

    #[test]
    fn counter_with_reset_max() {
        let counter = Counter::new();
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
