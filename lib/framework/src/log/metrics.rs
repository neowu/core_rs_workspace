use std::fs;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use chrono::DateTime;
use chrono::Utc;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use crate::console;
use crate::exception::Severity;
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

impl Metrics {
    fn update_error(&mut self, severity: Severity, error_code: &'static str, error_message: String) {
        if self.error.as_ref().is_none_or(|error| error.severity < severity) {
            self.error = Some(Error { severity, code: Some(error_code), message: error_message });
        }
    }
}

type Collector = Box<dyn Fn(&mut Metrics) + Send>;

pub struct MetricsCollector {
    cpu_stats: Option<CpuStats>,
    mem_stats: Option<MemoryStats>,
    collectors: Vec<Collector>,
}

impl MetricsCollector {
    pub fn new() -> Self {
        let now = Instant::now();
        let cpu_stats = if let Some(clock_ticks) = clock_ticks()
            && let Some(container_cpu_time) = container_cpu_time()
            && let Some(process_cpu_time) = process_cpu_time(clock_ticks)
            && let Some(cpu_max) = container_cpu_max()
        {
            Some(CpuStats {
                previous_wall_time: now,
                previous_container_cpu_time: container_cpu_time,
                previous_process_cpu_time: process_cpu_time,
                cpu_max,
                clock_ticks,
            })
        } else {
            None
        };

        let mem_stats = if let Some(page_size) = page_size()
            && let Some(max) = container_mem_max()
        {
            Some(MemoryStats { max, page_size })
        } else {
            None
        };

        Self { cpu_stats, mem_stats, collectors: Vec::new() }
    }

    pub fn add(&mut self, collector: impl Fn(&mut Metrics) + Send + 'static) {
        self.collectors.push(Box::new(collector));
    }

    pub async fn start(mut self, shutdown_signal: CancellationToken) {
        if let Some(Context { app, appender }) = CONTEXT.get() {
            console!("metrics collector started");
            loop {
                tokio::select! {
                    () = shutdown_signal.cancelled() => {
                        console!("metrics collector stopped");
                        return;
                    }
                    () = sleep(Duration::from_secs(5)) => {
                        let metrics = self.collect_metrics();
                        appender.append_metrics(&metrics, app);
                    }
                }
            }
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

        if let Some(cpu_stats) = &mut self.cpu_stats {
            collect_cpu_usage(&mut metrics, cpu_stats);
        }

        if let Some(mem_stats) = &self.mem_stats {
            collect_mem_usage(&mut metrics, mem_stats);
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

struct CpuStats {
    previous_wall_time: Instant,
    previous_container_cpu_time: u64,
    previous_process_cpu_time: u64,
    cpu_max: f64,
    clock_ticks: u64,
}

impl CpuStats {
    fn usage(&self, wall_elapsed: u64, prev: u64, current: u64) -> f64 {
        let cpu_used = current.saturating_sub(prev) as f64 / wall_elapsed as f64;
        cpu_used / self.cpu_max
    }
}

struct MemoryStats {
    max: u64,
    page_size: u64,
}

impl MemoryStats {
    fn usage(&self, used: u64) -> f64 {
        used as f64 / self.max as f64
    }
}

// collects cpu/memory usage in docker with cgroup v2 (the only supported env)
fn collect_cpu_usage(metrics: &mut Metrics, cpu_stats: &mut CpuStats) {
    let now = Instant::now();
    let wall_elapsed = now.duration_since(cpu_stats.previous_wall_time).as_micros() as u64;
    if wall_elapsed == 0 {
        return;
    }
    let (Some(container_cpu_time), Some(process_cpu_time)) =
        (container_cpu_time(), process_cpu_time(cpu_stats.clock_ticks))
    else {
        return;
    };

    let container_usage = cpu_stats.usage(wall_elapsed, cpu_stats.previous_container_cpu_time, container_cpu_time);
    let process_usage = cpu_stats.usage(wall_elapsed, cpu_stats.previous_process_cpu_time, process_cpu_time);

    // update previous stats
    cpu_stats.previous_wall_time = now;
    cpu_stats.previous_container_cpu_time = container_cpu_time;
    cpu_stats.previous_process_cpu_time = process_cpu_time;

    metrics.stats.push(("container_cpu_usage", (container_usage * 1000.0).round() as u64));
    metrics.stats.push(("process_cpu_usage", (process_usage * 1000.0).round() as u64));

    if container_usage > 0.8 {
        metrics.update_error(
            Severity::Warn,
            "HIGH_CPU_USAGE",
            format!("cpu usage is high, usage={:.2}%", container_usage * 100.0),
        );
        metrics.info.push(("cpu_pressure", fs::read_to_string("/sys/fs/cgroup/cpu.pressure").unwrap_or_default()));
    }
}

fn collect_mem_usage(metrics: &mut Metrics, mem_stats: &MemoryStats) {
    metrics.stats.push(("container_mem_max", mem_stats.max));

    if let Some(vm_rss) = process_vm_rss(mem_stats.page_size) {
        metrics.stats.push(("process_vm_rss", vm_rss));
    }

    if let Some(container_mem_used) = container_mem_used() {
        metrics.stats.push(("container_mem_used", container_mem_used));

        let mem_usage = mem_stats.usage(container_mem_used);
        if mem_usage > 0.8 {
            metrics.update_error(
                Severity::Warn,
                "HIGH_MEM_USAGE",
                format!("memory usage is high, usage={:.2}%", mem_usage * 100.0),
            );
            metrics.info.push(("proc_status", fs::read_to_string("/proc/self/status").unwrap_or_default()));
            metrics.info.push(("memory_stat", fs::read_to_string("/sys/fs/cgroup/memory.stat").unwrap_or_default()));
        }
    }
}

// container working set memory (memory.current - inactive_file) in bytes, cgroup v2
fn container_mem_used() -> Option<u64> {
    let current = parse_u64(fs::read_to_string("/sys/fs/cgroup/memory.current").ok()?.trim()).ok()?;
    let content = fs::read_to_string("/sys/fs/cgroup/memory.stat").ok()?;
    let inactive_file = content
        .lines()
        .find_map(|line| line.strip_prefix("inactive_file "))
        .and_then(|value| parse_u64(value.trim()).ok())?;
    Some(current.saturating_sub(inactive_file))
}

// resident set size (RSS) in bytes from /proc/self/statm
fn process_vm_rss(page_size: u64) -> Option<u64> {
    let content = fs::read_to_string("/proc/self/statm").ok()?;
    let resident = parse_u64(content.split_whitespace().nth(1)?).ok()?;
    Some(resident * page_size)
}

pub fn container_mem_max() -> Option<u64> {
    let content = fs::read_to_string("/sys/fs/cgroup/memory.max").ok()?;
    let content = content.trim();
    if content != "max" {
        return parse_u64(content).ok();
    }
    let pages = unsafe { libc::sysconf(libc::_SC_PHYS_PAGES) };
    let page_size = page_size()?;
    (page_size > 0).then(|| pages as u64 * page_size)
}

fn page_size() -> Option<u64> {
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    (page_size > 0).then_some(page_size as u64)
}

fn clock_ticks() -> Option<u64> {
    let clock_ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    (clock_ticks > 0).then_some(clock_ticks as u64)
}

// process cpu time (utime + stime) from /proc/self/stat, in microseconds
fn process_cpu_time(clock_ticks: u64) -> Option<u64> {
    let content = fs::read_to_string("/proc/self/stat").ok()?;
    // comm (field 2) The filename of the executable in parentheses, so split after the last ')'
    let mut fields = content.rsplit_once(')')?.1.split_whitespace();
    // after ')': index 0 = state (field 3); utime = field 14 -> nth(11), stime = next
    let user_time = parse_u64(fields.nth(11)?).ok()?;
    let sys_time = parse_u64(fields.next()?).ok()?;
    Some((user_time + sys_time) * 1_000_000 / clock_ticks)
}

// only for docker w/ cgroup v2, for entire container
fn container_cpu_time() -> Option<u64> {
    let content = fs::read_to_string("/sys/fs/cgroup/cpu.stat").ok()?;
    for line in content.lines() {
        if let Some(value) = line.strip_prefix("usage_usec ") {
            return parse_u64(value.trim()).ok();
        }
    }
    None
}

// percent of cpu quota (cpu.max), 100 = at the limit; percent of raw cores used if no quota set
fn container_cpu_max() -> Option<f64> {
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
