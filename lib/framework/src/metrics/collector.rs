use std::fs;
use std::time::Duration;
use std::time::Instant;

use tokio::sync::mpsc::UnboundedSender;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use crate::appender::Message;
use crate::log::Severity;
use crate::log::id_generator;
use crate::metrics::Metrics;
use crate::number::parse_u64;
use crate::time::DateTime;

type Collector = Box<dyn Fn(&mut Metrics) + Send>;

pub(crate) struct MetricsCollector {
    cpu_stats: Option<CpuStats>,
    mem_stats: Option<MemoryStats>,
    collectors: Vec<Collector>,
}

impl MetricsCollector {
    pub(crate) fn new() -> Self {
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

    pub(crate) fn add(&mut self, collector: impl Fn(&mut Metrics) + Send + 'static) {
        self.collectors.push(Box::new(collector));
    }

    pub(crate) async fn start(mut self, shutdown_signal: CancellationToken, sender: UnboundedSender<Message>) {
        console!("start metrics collector");
        loop {
            tokio::select! {
                () = shutdown_signal.cancelled() => {
                    console!("metrics collector stopped");
                    return;
                }
                () = sleep(Duration::from_secs(5)) => {
                    let metrics = self.collect_metrics();
                    let _result = sender.send(Message::Metrics(metrics.into()));
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

// collects cpu/memory usage in docker with cgroup v2, falls back to cgroup v1
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
        // pressure stall information is only exposed by cgroup v2
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
            metrics.info.push((
                "memory_stat",
                fs::read_to_string("/sys/fs/cgroup/memory.stat")
                    .or_else(|_| fs::read_to_string("/sys/fs/cgroup/memory/memory.stat"))
                    .unwrap_or_default(),
            ));
        }
    }
}

// container working set memory (current usage - inactive file cache) in bytes
fn container_mem_used() -> Option<u64> {
    // cgroup v2
    if let Some(current) = read_u64("/sys/fs/cgroup/memory.current") {
        let content = fs::read_to_string("/sys/fs/cgroup/memory.stat").ok()?;
        return Some(current.saturating_sub(stat_value(&content, "inactive_file")?));
    }
    // cgroup v1
    let current = read_u64("/sys/fs/cgroup/memory/memory.usage_in_bytes")?;
    let content = fs::read_to_string("/sys/fs/cgroup/memory/memory.stat").ok()?;
    Some(current.saturating_sub(stat_value(&content, "total_inactive_file")?))
}

fn read_u64(path: &str) -> Option<u64> {
    parse_u64(fs::read_to_string(path).ok()?.trim()).ok()
}

// reads "<key> <value>" from a cgroup stat file
fn stat_value(content: &str, key: &str) -> Option<u64> {
    content
        .lines()
        .find_map(|line| line.strip_prefix(key)?.strip_prefix(' '))
        .and_then(|value| parse_u64(value.trim()).ok())
}

// resident set size (RSS) in bytes from /proc/self/statm
fn process_vm_rss(page_size: u64) -> Option<u64> {
    let content = fs::read_to_string("/proc/self/statm").ok()?;
    let resident = parse_u64(content.split_whitespace().nth(1)?).ok()?;
    Some(resident * page_size)
}

fn container_mem_max() -> Option<u64> {
    // cgroup v2, "max" when there is no limit
    if let Ok(content) = fs::read_to_string("/sys/fs/cgroup/memory.max") {
        let content = content.trim();
        return if content == "max" { physical_mem() } else { parse_u64(content).ok() };
    }
    // cgroup v1, a huge sentinel value (not "max") when there is no limit
    let limit = read_u64("/sys/fs/cgroup/memory/memory.limit_in_bytes")?;
    match physical_mem() {
        Some(phys_mem) if limit > phys_mem => Some(phys_mem),
        _ => Some(limit),
    }
}

fn physical_mem() -> Option<u64> {
    let pages = unsafe { libc::sysconf(libc::_SC_PHYS_PAGES) };
    let page_size = page_size()?;
    (pages > 0).then(|| pages as u64 * page_size)
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

// cpu time of the entire container, in microseconds
fn container_cpu_time() -> Option<u64> {
    // cgroup v2
    if let Ok(content) = fs::read_to_string("/sys/fs/cgroup/cpu.stat") {
        return stat_value(&content, "usage_usec");
    }
    // cgroup v1, cpuacct.usage is in nanoseconds
    Some(read_u64("/sys/fs/cgroup/cpu,cpuacct/cpuacct.usage")? / 1000)
}

// percent of cpu quota, 100% = at the limit; percent of raw cores used if no quota set
fn container_cpu_max() -> Option<f64> {
    // cgroup v2, "max <period>" when there is no quota
    if let Ok(content) = fs::read_to_string("/sys/fs/cgroup/cpu.max") {
        let mut parts = content.split_whitespace();
        let quota = parts.next()?;
        if quota == "max" {
            return Some(1.0);
        }
        let quota = parse_u64(quota).ok()?;
        let period = parse_u64(parts.next()?).ok()?;
        return Some(quota as f64 / period as f64);
    }
    // cgroup v1, quota is -1 when there is no limit, and the cfs files may not be exposed at all
    let quota = read_u64("/sys/fs/cgroup/cpu,cpuacct/cpu.cfs_quota_us");
    let period = read_u64("/sys/fs/cgroup/cpu,cpuacct/cpu.cfs_period_us");
    match (quota, period) {
        (Some(quota), Some(period)) if period > 0 => Some(quota as f64 / period as f64),
        _ => Some(1.0),
    }
}
