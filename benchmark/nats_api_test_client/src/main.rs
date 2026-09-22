use std::time::Duration;
use std::time::Instant;

use async_nats::Client;
use async_nats::HeaderMap;
use async_nats::Message;
use bytes::Bytes;
use harness::stats;
use harness::stats::Recorder;
use harness::stats::Summary;
use nats_api_test_server::GetResponse;
use nats_api_test_server::PostRequest;
use nats_api_test_server::PostResponse;

use crate::args::Args;
use crate::args::Scenario;

mod args;

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

    println!(
        "scenario={}, subject={}, url={}, concurrency={}, threads={}, warmup={}s, duration={}s",
        args.scenario.as_str(),
        target.subject,
        args.url,
        args.concurrency,
        args.threads,
        args.warmup.as_secs(),
        args.duration.as_secs()
    );

    let mut warmup_requests = 0;
    if !args.warmup.is_zero() {
        let summary = load(&client, &target, args.concurrency, args.warmup).await;
        warmup_requests = summary.requests;
        println!("warmup done, requests={warmup_requests}");
    }

    let summary = load(&client, &target, args.concurrency, args.duration).await;
    stats::report(&summary);
    if args.record {
        stats::record(&config(&args), &summary, warmup_requests);
    }
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

/// What this client was asked to do, the prefix of the record line `run_nats_api_test.sh` folds
/// into the report.
fn config(args: &Args) -> String {
    format!(
        "scenario={} protocol=nats concurrency={} threads={} values={} warmup={} duration={}",
        args.scenario.as_str(),
        args.concurrency,
        args.threads,
        args.values,
        args.warmup.as_secs(),
        args.duration.as_secs()
    )
}
