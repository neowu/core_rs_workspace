use std::fs;
use std::time::Duration;
use std::time::Instant;

use tokio::sync::mpsc::UnboundedSender;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use crate::log::Severity;
use crate::log::id_generator;
use crate::metrics::Metrics;
use crate::metrics::appender::MetricsMessage;
use crate::number::parse_u64;
use crate::time::DateTime;

type Collector = Box<dyn Fn(&mut Metrics) + Send>;

#[derive(Default)]
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

    pub(crate) async fn start(mut self, shutdown_signal: CancellationToken, sender: UnboundedSender<MetricsMessage>) {
        console!("start metrics collector");
        loop {
            tokio::select! {
                () = shutdown_signal.cancelled() => {
                    console!("metrics collector stopped");
                    return;
                }
                () = sleep(Duration::from_secs(5)) => {
                    let metrics = self.collect_metrics();
                    let _result = sender.send(metrics.into());
                }
            }
        }
    }

    fn collect_metrics(&mut self) -> Metrics {
        let timestamp = DateTime::now();
        let mut metrics = Metrics {
            id: id_generator::next_id(timestamp.unix_timestamp_millis()),
            timestamp,
            severity: Severity::Info,
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

    metrics.stats.push(("container_cpu_usage", (container_usage * 100.0).round() as u64));
    metrics.stats.push(("process_cpu_usage", (process_usage * 100.0).round() as u64));

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

fn container_mem_max() -> Option<u64> {
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

// percent of cpu quota (cpu.max), 100% = at the limit; percent of raw cores used if no quota set
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
