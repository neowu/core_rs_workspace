use std::time::Duration;

use serde_json::Value;
use serde_json::json;

/// Per worker recording, merged once at the end so the measured loop never touches shared state.
#[derive(Default)]
pub struct Recorder {
    pub latencies: Vec<u64>,
    pub failed: u64,
    pub errors: u64,
}

impl Recorder {
    pub fn record(&mut self, elapsed: u64) {
        self.latencies.push(elapsed);
    }
}

pub struct Summary {
    pub requests: u64,
    pub failed: u64,
    pub errors: u64,
    pub elapsed: Duration,
    latencies: Vec<u64>,
}

impl Summary {
    pub fn merge(recorders: Vec<Recorder>, elapsed: Duration) -> Self {
        let mut latencies = Vec::with_capacity(recorders.iter().map(|recorder| recorder.latencies.len()).sum());
        let mut failed = 0;
        let mut errors = 0;
        for mut recorder in recorders {
            latencies.append(&mut recorder.latencies);
            failed += recorder.failed;
            errors += recorder.errors;
        }
        latencies.sort_unstable();
        Summary { requests: latencies.len() as u64, failed, errors, elapsed, latencies }
    }

    pub fn throughput(&self) -> f64 {
        let seconds = self.elapsed.as_secs_f64();
        if seconds > 0.0 { self.requests as f64 / seconds } else { 0.0 }
    }

    pub fn mean(&self) -> f64 {
        if self.latencies.is_empty() {
            return 0.0;
        }
        self.latencies.iter().sum::<u64>() as f64 / self.latencies.len() as f64
    }

    /// Nearest rank on the sorted samples, exact rather than an estimate since every sample is kept.
    pub fn percentile(&self, percentile: f64) -> u64 {
        if self.latencies.is_empty() {
            return 0;
        }
        let rank = (percentile / 100.0 * (self.latencies.len() - 1) as f64).round() as usize;
        self.latencies[rank.min(self.latencies.len() - 1)]
    }
}

/// The measurement as it goes into the result file the client writes under `--output`.
pub fn result(summary: &Summary) -> Value {
    json!({
        "requests": summary.requests,
        "failed": summary.failed,
        "errors": summary.errors,
        "elapsed": round(summary.elapsed.as_secs_f64(), 3),
        "throughput": summary.throughput().round() as u64,
        "mean_ms": round(millis(summary.mean()), 3),
        "p50_ms": round(millis(summary.percentile(50.0) as f64), 3),
        "p90_ms": round(millis(summary.percentile(90.0) as f64), 3),
        "p99_ms": round(millis(summary.percentile(99.0) as f64), 3),
        "p999_ms": round(millis(summary.percentile(99.9) as f64), 3),
        "max_ms": round(millis(summary.percentile(100.0) as f64), 3),
    })
}

pub fn round(value: f64, digits: i32) -> f64 {
    let scale = 10_f64.powi(digits);
    (value * scale).round() / scale
}

fn millis(nanos: f64) -> f64 {
    nanos / 1_000_000.0
}
