use std::fs::write;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use framework::exception::Exception;
use nats_api_test_server::BenchmarkService as _;
use nats_api_test_server::BenchmarkServiceClient;
use nats_api_test_server::GetRequest;
use nats_api_test_server::PostRequest;
use nats_api_test_server::info::MachineInfo;
use nats_api_test_server::info::ProcessUsage;
use nats_api_test_server::info::ServerInfo;
use serde_json::json;

use crate::args::Args;
use crate::args::Scenario;
use crate::stats::Recorder;
use crate::stats::Summary;

mod args;
mod stats;

const ID: i64 = 7;
const NO_RESPONDERS: &str = "NATS_NO_RESPONDERS";
const TIMEOUT: &str = "NATS_TIMEOUT";

fn main() {
    let args = Args::parse();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.threads)
        .enable_all()
        .build()
        .expect("failed to build runtime");

    runtime.block_on(run(args));
}

#[derive(Clone, Copy)]
struct Target {
    scenario: Scenario,
    values: usize,
}

async fn run(args: Args) {
    // one connection, as a framework app holds framework_nats::connect: every request rides it
    // multiplexed, replies come back on the one shared inbox subscription
    let nats_client = framework_nats::connect(&args.url).await;
    // the client #[nats_api] generates, what a framework app calls the service with. Without an
    // action context its logging is a no-op, so the measured loop pays only for encoding the
    // request and decoding the reply
    let client = Arc::new(BenchmarkServiceClient::new(nats_client.clone()));

    let target = Target { scenario: args.scenario, values: args.values };
    verify(&client, target).await;

    if !args.warmup.is_zero() {
        load(&client, target, args.concurrency, args.warmup).await;
        println!("warmup done");
    }

    // sampled around the measured phase only, so server cpu per request excludes the warmup
    let before = server_info(&client).await;
    let client_before = ProcessUsage::current();
    let summary = load(&client, target, args.concurrency, args.duration).await;
    let client_after = ProcessUsage::current();
    let after = server_info(&client).await;

    let cpu_us = after.usage.cpu_us.saturating_sub(before.usage.cpu_us);
    let cpu_us_per_request = if summary.requests > 0 { cpu_us as f64 / summary.requests as f64 } else { 0.0 };
    let peak_rss_mb = after.usage.peak_rss_kb as f64 / 1024.0;
    // share of all the host's cores, a side near 100 is the one holding the rate down
    let server_cpu_pct = cpu_pct(cpu_us, summary.elapsed, after.machine.cores);
    let client_machine = MachineInfo::collect();
    let client_cpu_pct =
        cpu_pct(client_after.cpu_us.saturating_sub(client_before.cpu_us), summary.elapsed, client_machine.cores);

    let path = &args.output;
    let result = json!({
        "config": config(&args),
        "result": stats::result(&summary),
        "server": {
            "machine": after.machine,
            "threads": after.threads,
            "cpu_us_per_request": stats::round(cpu_us_per_request, 2),
            "cpu_pct": stats::round(server_cpu_pct, 1),
            "peak_rss_mb": stats::round(peak_rss_mb, 1),
        },
        "client": {
            "machine": client_machine,
            "cpu_pct": stats::round(client_cpu_pct, 1),
        },
        "broker": {
            "url": args.url,
            "version": nats_client.server_info().version,
        },
    });
    let text = serde_json::to_string_pretty(&result).expect("failed to serialize result");
    write(path, text).unwrap_or_else(|err| panic!("failed to write result, path={path}, err={err}"));
    println!("result written to {path}");
}

async fn server_info(client: &BenchmarkServiceClient) -> ServerInfo {
    client.info().await.unwrap_or_else(|err| panic!("failed to get server info, err={err:?}"))
}

/// Closed loop: every worker holds one in flight request and sends the next as soon as the reply
/// arrives, so `concurrency` is the number of outstanding requests and the server's own pace sets
/// the rate.
async fn load(client: &Arc<BenchmarkServiceClient>, target: Target, concurrency: usize, duration: Duration) -> Summary {
    let start = Instant::now();
    let deadline = start + duration;

    let mut handles = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        handles.push(tokio::spawn(worker(Arc::clone(client), target, deadline)));
    }

    let mut recorders = Vec::with_capacity(concurrency);
    for handle in handles {
        recorders.push(handle.await.expect("worker cannot panic"));
    }

    Summary::merge(recorders, start.elapsed())
}

async fn worker(client: Arc<BenchmarkServiceClient>, target: Target, deadline: Instant) -> Recorder {
    let mut recorder = Recorder::default();
    loop {
        let start = Instant::now();
        if start >= deadline {
            break;
        }
        match request(&client, target).await {
            Ok(()) => recorder.record(start.elapsed().as_nanos() as u64),
            // no reply at all is an error, anything else is a reply that failed
            Err(err) if matches!(err.code, Some(NO_RESPONDERS | TIMEOUT)) => recorder.errors += 1,
            Err(_) => recorder.failed += 1,
        }
    }
    recorder
}

async fn request(client: &BenchmarkServiceClient, target: Target) -> Result<(), Exception> {
    match target.scenario {
        Scenario::Get => client.get(GetRequest { id: ID }).await.map(drop),
        Scenario::Post => client.post(post_request(target.values)).await.map(drop),
    }
}

fn post_request(values: usize) -> PostRequest {
    PostRequest { id: ID, name: "benchmark".to_owned(), values: vec![1; values] }
}

/// Waits for the service to subscribe, then sends one request of the selected scenario and checks
/// the reply, so a broken server fails here instead of producing a fast benchmark of errors.
async fn verify(client: &BenchmarkServiceClient, target: Target) {
    let scenario = target.scenario.as_str();
    let mut ready = false;
    // no responders until the service subscribes, which is this protocol's readiness signal
    for _ in 0..100 {
        match request(client, target).await {
            Ok(()) => {
                ready = true;
                break;
            }
            Err(err) if err.code == Some(NO_RESPONDERS) => tokio::time::sleep(Duration::from_millis(100)).await,
            Err(err) => panic!("failed to call, scenario={scenario}, err={err:?}"),
        }
    }
    assert!(ready, "service did not start, scenario={scenario}");

    match target.scenario {
        Scenario::Get => {
            let response = client.get(GetRequest { id: ID }).await.expect("failed to call get");
            assert_eq!(response.id, ID, "get subject must echo the id");
        }
        Scenario::Post => {
            let response = client.post(post_request(target.values)).await.expect("failed to call post");
            assert_eq!(response.id, ID, "post subject must echo the id");
            assert_eq!(response.sum, target.values as i64, "post subject must sum the values");
        }
    }

    println!("verified, scenario={scenario}");
}

/// What this client was asked to do, the `config` part of the result file.
fn config(args: &Args) -> serde_json::Value {
    json!({
        "scenario": args.scenario.as_str(),
        "protocol": "nats",
        "url": args.url,
        "concurrency": args.concurrency,
        "threads": args.threads,
        "values": args.values,
        "warmup": args.warmup.as_secs(),
        "duration": args.duration.as_secs(),
    })
}

fn cpu_pct(cpu_us: u64, elapsed: Duration, cores: usize) -> f64 {
    let available = elapsed.as_secs_f64() * 1_000_000.0 * cores.max(1) as f64;
    if available > 0.0 { cpu_us as f64 * 100.0 / available } else { 0.0 }
}
