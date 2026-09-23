use std::fs::write;
use std::time::Duration;
use std::time::Instant;

use async_nats::Client;
use async_nats::HeaderMap;
use async_nats::Message;
use bytes::Bytes;
use nats_api_test_server::GetResponse;
use nats_api_test_server::PostRequest;
use nats_api_test_server::PostResponse;
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
const CLIENT: &str = "client";
const REF_ID: &str = "ref_id";
const ERROR: &str = "error";

fn main() {
    let args = Args::parse();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.threads)
        .enable_all()
        .build()
        .expect("failed to build runtime");

    runtime.block_on(run(args));
}

/// One prepared request, built once per run so the measured loop does no serialization, only what
/// it takes to put bytes on the connection.
#[derive(Clone)]
struct Target {
    scenario: Scenario,
    subject: &'static str,
    payload: Bytes,
    headers: HeaderMap,
}

impl Target {
    fn new(args: &Args) -> Self {
        let scenario = args.scenario;
        let payload = if scenario.is_post() {
            let request = PostRequest { id: ID, name: "benchmark".to_owned(), values: vec![1; args.values] };
            serde_json::to_vec(&request).expect("failed to serialize payload")
        } else {
            format!(r#"{{"id":{ID}}}"#).into_bytes()
        };

        // what framework_nats::link_context puts on every internal call, so the service does the
        // same header work it does in production -- reading them is part of what is measured
        let mut headers = HeaderMap::new();
        headers.insert(CLIENT, env!("CARGO_PKG_NAME"));
        headers.insert(REF_ID, "benchmark");

        Target { scenario, subject: scenario.subject(), payload: Bytes::from(payload), headers }
    }
}

async fn run(args: Args) {
    // one connection, matching how a framework app holds framework_nats::connect: every request
    // rides it multiplexed, replies come back on the one shared inbox subscription
    let client = async_nats::connect(&args.url)
        .await
        .unwrap_or_else(|err| panic!("failed to connect, url={}, err={err}", args.url));

    let target = Target::new(&args);
    verify(&client, &target, args.values).await;

    if !args.warmup.is_zero() {
        load(&client, &target, args.concurrency, args.warmup).await;
        println!("warmup done");
    }

    // sampled around the measured phase only, so server cpu per request excludes the warmup
    let before = server_info(&client).await;
    let client_before = ProcessUsage::current();
    let summary = load(&client, &target, args.concurrency, args.duration).await;
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
            "version": client.server_info().version,
        },
    });
    let text = serde_json::to_string_pretty(&result).expect("failed to serialize result");
    write(path, text).unwrap_or_else(|err| panic!("failed to write result, path={path}, err={err}"));
    println!("result written to {path}");
}

async fn server_info(client: &Client) -> ServerInfo {
    let subject = nats_api_test_server::INFO;
    let reply = client
        .request(subject, Bytes::from_static(b"null"))
        .await
        .unwrap_or_else(|err| panic!("failed to call, subject={subject}, err={err}"));
    assert!(!is_error(&reply), "unexpected error reply, subject={subject}");
    serde_json::from_slice(&reply.payload)
        .unwrap_or_else(|err| panic!("failed to parse server info, subject={subject}, err={err}"))
}

/// Closed loop: every worker holds one in flight request and sends the next as soon as the reply
/// arrives, so `concurrency` is the number of outstanding requests and the server's own pace sets
/// the rate.
async fn load(client: &Client, target: &Target, concurrency: usize, duration: Duration) -> Summary {
    let start = Instant::now();
    let deadline = start + duration;

    let mut handles = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        handles.push(tokio::spawn(worker(client.clone(), target.clone(), deadline)));
    }

    let mut recorders = Vec::with_capacity(concurrency);
    for handle in handles {
        recorders.push(handle.await.expect("worker cannot panic"));
    }

    Summary::merge(recorders, start.elapsed())
}

async fn worker(client: Client, target: Target, deadline: Instant) -> Recorder {
    let mut recorder = Recorder::default();
    loop {
        let start = Instant::now();
        if start >= deadline {
            break;
        }
        match request(&client, &target).await {
            Ok(reply) => {
                if is_error(&reply) {
                    recorder.failed += 1;
                } else {
                    recorder.record(start.elapsed().as_nanos() as u64);
                }
            }
            Err(_) => recorder.errors += 1,
        }
    }
    recorder
}

/// Cloning is a refcount bump on the payload and a two entry map copy for the headers, which is
/// what `ServiceClient` builds per call anyway.
async fn request(client: &Client, target: &Target) -> Result<Message, async_nats::RequestError> {
    client.request_with_headers(target.subject, target.headers.clone(), target.payload.clone()).await
}

/// The framework marks an error reply with a header rather than a status, so this is the status
/// check: one lookup, no parsing.
fn is_error(reply: &Message) -> bool {
    reply.headers.as_ref().is_some_and(|headers| headers.get(ERROR).is_some())
}

/// Waits for the service to subscribe, then sends one request of the selected scenario and parses
/// the reply, so a wrong subject or a broken server fails here instead of producing a fast
/// benchmark of errors. The measured loop never parses.
async fn verify(client: &Client, target: &Target, values: usize) {
    let subject = target.subject;
    let mut reply = None;
    // no responders until the service subscribes, which is this protocol's readiness signal
    for _ in 0..100 {
        match request(client, target).await {
            Ok(message) => {
                reply = Some(message);
                break;
            }
            Err(err) if err.kind() == async_nats::RequestErrorKind::NoResponders => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(err) => panic!("failed to call, subject={subject}, err={err}"),
        }
    }
    let reply = reply.unwrap_or_else(|| panic!("service did not start, subject={subject}"));

    let body = String::from_utf8(reply.payload.to_vec()).expect("reply is not utf8");
    assert!(!is_error(&reply), "unexpected error reply, subject={subject}, body={body}");

    if target.scenario.is_post() {
        let response: PostResponse = serde_json::from_str(&body).expect("failed to parse post reply");
        assert_eq!(response.id, ID, "post subject must echo the id");
        assert_eq!(response.sum, values as i64, "post subject must sum the values");
    } else {
        let response: GetResponse = serde_json::from_str(&body).expect("failed to parse get reply");
        assert_eq!(response.id, ID, "get subject must echo the id");
    }

    println!("verified, subject={subject}");
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
