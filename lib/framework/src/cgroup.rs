//! Container limits and usage, read from cgroup v2 with a v1 fallback.

use std::fs;

use crate::number::parse_u64;

// container working set memory (current usage - inactive file cache) in bytes
pub(crate) fn container_mem_used() -> Option<u64> {
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

pub(crate) fn container_mem_max() -> Option<u64> {
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

pub(crate) fn page_size() -> Option<u64> {
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    (page_size > 0).then_some(page_size as u64)
}

// cpu time of the entire container, in microseconds
pub(crate) fn container_cpu_time() -> Option<u64> {
    // cgroup v2
    if let Ok(content) = fs::read_to_string("/sys/fs/cgroup/cpu.stat") {
        return stat_value(&content, "usage_usec");
    }
    // cgroup v1, cpuacct.usage is in nanoseconds
    Some(read_u64("/sys/fs/cgroup/cpu,cpuacct/cpuacct.usage")? / 1000)
}

// percent of cpu quota, 100% = at the limit; percent of raw cores used if no quota set
pub(crate) fn container_cpu_max() -> Option<f64> {
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
