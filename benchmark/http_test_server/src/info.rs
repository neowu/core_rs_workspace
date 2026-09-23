use std::fs::read_to_string;
use std::process::Command;
use std::thread::available_parallelism;

use serde::Deserialize;
use serde::Serialize;

/// The machine a process runs on, collected once at startup. Commands assume debian, every field
/// falls back to `unknown` rather than failing a run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MachineInfo {
    pub host: String,
    pub ip: String,
    pub cpu: String,
    pub cores: usize,
    pub memory_mb: u64,
    pub os: String,
}

impl MachineInfo {
    pub fn collect() -> Self {
        MachineInfo {
            host: output("hostname", &[]).unwrap_or_else(unknown),
            ip: output("hostname", &["-I"])
                .and_then(|ips| ips.split_whitespace().next().map(str::to_owned))
                .unwrap_or_else(unknown),
            cpu: output("lscpu", &[])
                .and_then(|text| {
                    text.lines().find_map(|line| line.strip_prefix("Model name:").map(|name| name.trim().to_owned()))
                })
                .unwrap_or_else(unknown),
            cores: available_parallelism().map_or(0, |n| n.get()),
            memory_mb: read_to_string("/proc/meminfo")
                .ok()
                .and_then(|text| {
                    text.lines()
                        .find_map(|line| line.strip_prefix("MemTotal:"))
                        .and_then(|value| value.trim().trim_end_matches("kB").trim().parse::<u64>().ok())
                })
                .map_or(0, |kb| kb / 1024),
            os: output("uname", &["-sr"]).unwrap_or_else(unknown),
        }
    }
}

/// What the server reports on its info endpoint: the machine, plus its own usage at the moment of
/// the call, so the client can take the delta around the measured phase.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerInfo {
    pub machine: MachineInfo,
    pub threads: usize,
    pub usage: ProcessUsage,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ProcessUsage {
    /// user plus system cpu time of the whole process
    pub cpu_us: u64,
    pub peak_rss_kb: u64,
}

impl ProcessUsage {
    pub fn current() -> Self {
        // SAFETY: getrusage only writes into the zeroed struct it is given
        let usage = unsafe {
            let mut usage: libc::rusage = std::mem::zeroed();
            libc::getrusage(libc::RUSAGE_SELF, &raw mut usage);
            usage
        };
        let micros = |time: libc::timeval| time.tv_sec as u64 * 1_000_000 + time.tv_usec as u64;
        // linux reports ru_maxrss in kB, macos in bytes
        let peak_rss_kb =
            if cfg!(target_os = "macos") { usage.ru_maxrss as u64 / 1024 } else { usage.ru_maxrss as u64 };
        ProcessUsage { cpu_us: micros(usage.ru_utime) + micros(usage.ru_stime), peak_rss_kb }
    }
}

fn output(program: &str, args: &[&str]) -> Option<String> {
    let out = Command::new(program).args(args).output().ok()?;
    if !out.status.success() {
        return None;
    }
    let text = String::from_utf8(out.stdout).ok()?.trim().to_owned();
    if text.is_empty() { None } else { Some(text) }
}

fn unknown() -> String {
    "unknown".to_owned()
}
