use std::time::Duration;

use crate::args::Args;

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

pub fn report(summary: &Summary) {
    println!("--- result ---");
    println!("elapsed={:.1}s", summary.elapsed.as_secs_f64());
    println!("requests={}, failed={}, errors={}", summary.requests, summary.failed, summary.errors);
    println!("throughput={:.0}/s", summary.throughput());
    println!(
        "latency(ms): mean={:.3}, p50={:.3}, p90={:.3}, p99={:.3}, p99.9={:.3}, max={:.3}",
        millis(summary.mean()),
        millis(summary.percentile(50.0) as f64),
        millis(summary.percentile(90.0) as f64),
        millis(summary.percentile(99.0) as f64),
        millis(summary.percentile(99.9) as f64),
        millis(summary.percentile(100.0) as f64)
    );
}

/// One machine readable line for `run.sh` to fold into the report, so the report is never built by
/// scraping the human output above.
pub fn record(args: &Args, summary: &Summary, warmup_requests: u64) {
    println!(
        "data scenario={} protocol=h2c concurrency={} threads={} values={} warmup={} duration={} warmup_requests={} \
         requests={} failed={} errors={} elapsed={:.3} throughput={:.0} mean_ms={:.3} p50_ms={:.3} \
         p90_ms={:.3} p99_ms={:.3} p999_ms={:.3} max_ms={:.3}",
        args.scenario.as_str(),
        args.concurrency,
        args.threads,
        args.values,
        args.warmup.as_secs(),
        args.duration.as_secs(),
        warmup_requests,
        summary.requests,
        summary.failed,
        summary.errors,
        summary.elapsed.as_secs_f64(),
        summary.throughput(),
        millis(summary.mean()),
        millis(summary.percentile(50.0) as f64),
        millis(summary.percentile(90.0) as f64),
        millis(summary.percentile(99.0) as f64),
        millis(summary.percentile(99.9) as f64),
        millis(summary.percentile(100.0) as f64)
    );
}

fn millis(nanos: f64) -> f64 {
    nanos / 1_000_000.0
}
