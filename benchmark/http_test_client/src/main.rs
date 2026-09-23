use std::fs::write;
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use http_test_server::DbInsertRequest;
use http_test_server::DbInsertResponse;
use http_test_server::DbSelectResponse;
use http_test_server::GetResponse;
use http_test_server::InitDbRequest;
use http_test_server::InitDbResponse;
use http_test_server::PostRequest;
use http_test_server::PostResponse;
use http_test_server::info::MachineInfo;
use http_test_server::info::ProcessUsage;
use http_test_server::info::ServerInfo;
use reqwest::Body;
use reqwest::Client;
use reqwest::Method;
use reqwest::Request;
use reqwest::Url;
use reqwest::Version;
use reqwest::header::CONTENT_TYPE;
use reqwest::header::HeaderValue;
use serde_json::json;

use crate::args::Args;
use crate::args::Scenario;
use crate::stats::Recorder;
use crate::stats::Summary;

mod args;
mod stats;

const ID: i64 = 7;

fn main() {
    let args = Args::parse();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.threads)
        .enable_all()
        .build()
        .expect("failed to build runtime");

    runtime.block_on(run(args));
}

/// One prepared request, built once per run so the measured loop does no url parsing and no
/// serialization, only what it takes to put bytes on a connection.
#[derive(Clone)]
struct Target {
    scenario: Scenario,
    method: Method,
    url: Url,
    body: Option<Bytes>,
}

impl Target {
    fn new(args: &Args) -> Self {
        let scenario = args.scenario;
        let path = scenario.path();
        if scenario.is_post() {
            let body = match scenario {
                // one id past the seeded rows: the first insert lands, every later one conflicts
                Scenario::DbInsertIgnore => serde_json::to_vec(&DbInsertRequest {
                    id: args.rows + 1,
                    name: "benchmark".to_owned(),
                    amount: 100,
                }),
                _ => serde_json::to_vec(&PostRequest {
                    id: ID,
                    name: "benchmark".to_owned(),
                    values: vec![1; args.values],
                }),
            }
            .expect("failed to serialize body");
            Target {
                scenario,
                method: Method::POST,
                url: url(&format!("{}{path}", args.url)),
                body: Some(Bytes::from(body)),
            }
        } else {
            Target { scenario, method: Method::GET, url: url(&format!("{}{path}?id={ID}", args.url)), body: None }
        }
    }

    fn request(&self) -> Request {
        let mut request = Request::new(self.method.clone(), self.url.clone());
        if let Some(ref body) = self.body {
            request.headers_mut().insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
            *request.body_mut() = Some(Body::from(body.clone()));
        }
        request
    }
}

async fn run(args: Args) {
    // h2c, matching how framework::http::HttpClient is configured for internal calls
    // (HttpClientConfig::internal_only -> prefer_http2 -> http2_prior_knowledge): one shared,
    // kept alive connection carrying every request as a multiplexed stream
    let client = Client::builder()
        .http2_prior_knowledge()
        .pool_idle_timeout(Duration::from_secs(300))
        .connection_verbose(false)
        .timeout(Duration::from_secs(10))
        .build()
        .expect("failed to build client");

    if args.scenario.is_db() {
        init_db(&client, &args.url, args.rows).await;
    }

    let target = Target::new(&args);
    verify(&client, &target, &args).await;

    if !args.warmup.is_zero() {
        load(&client, &target, args.concurrency, args.warmup).await;
        println!("warmup done");
    }

    // sampled around the measured phase only, so server cpu per request excludes the warmup
    let before = server_info(&client, &args.url).await;
    let client_before = ProcessUsage::current();
    let summary = load(&client, &target, args.concurrency, args.duration).await;
    let client_after = ProcessUsage::current();
    let after = server_info(&client, &args.url).await;

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
    });
    let text = serde_json::to_string_pretty(&result).expect("failed to serialize result");
    write(path, text).unwrap_or_else(|err| panic!("failed to write result, path={path}, err={err}"));
    println!("result written to {path}");
}

async fn server_info(client: &Client, base_url: &str) -> ServerInfo {
    let address = format!("{base_url}/benchmark/info");
    let (_, body) = execute(client, Request::new(Method::GET, url(&address)), &address).await;
    serde_json::from_str(&body).unwrap_or_else(|err| panic!("failed to parse server info, url={address}, err={err}"))
}

/// Closed loop: every worker holds one in flight request and sends the next as soon as the previous
/// one resolves, so `concurrency` is the number of open h2 streams and the server's own pace sets
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
        match client.execute(target.request()).await {
            Ok(response) => {
                let status = response.status();
                // the body must be drained, otherwise the connection is not returned to the pool
                let body = response.bytes().await;
                if status.is_success() && body.is_ok() {
                    recorder.record(start.elapsed().as_nanos() as u64);
                } else {
                    recorder.failed += 1;
                }
            }
            Err(_) => recorder.errors += 1,
        }
    }
    recorder
}

/// Sends one request of the selected scenario and parses the response, so a wrong url or a broken
/// server fails here instead of producing a fast benchmark of 404s. The measured loop never parses.
async fn verify(client: &Client, target: &Target, args: &Args) {
    let url = target.url.as_str();
    let (version, body) = execute(client, target.request(), url).await;

    // a silent fallback to http/1.1 would quietly benchmark a protocol nobody runs internally
    assert_eq!(version, Version::HTTP_2, "server must speak h2c, url={url}");

    match target.scenario {
        Scenario::DbSelect => {
            let response: DbSelectResponse = serde_json::from_str(&body).expect("failed to parse select response");
            assert_eq!(response.id, ID, "select endpoint must return the seeded row");
        }
        Scenario::DbInsertIgnore => {
            let response: DbInsertResponse = serde_json::from_str(&body).expect("failed to parse insert response");
            assert!(response.inserted, "first insert after init_db must land, id={}", response.id);
        }
        Scenario::Post | Scenario::ApiPost => {
            let response: PostResponse = serde_json::from_str(&body).expect("failed to parse post response");
            assert_eq!(response.id, ID, "post endpoint must echo the id");
            assert_eq!(response.sum, args.values as i64, "post endpoint must sum the values");
        }
        Scenario::Get | Scenario::ApiGet => {
            let response: GetResponse = serde_json::from_str(&body).expect("failed to parse get response");
            assert_eq!(response.id, ID, "get endpoint must echo the id");
        }
    }

    println!("verified, url={url}, protocol=h2c");
}

/// Drops and recreates the table, so every db run starts from the same state.
async fn init_db(client: &Client, base_url: &str, rows: i64) {
    let address = format!("{base_url}/benchmark/init_db");
    let mut request = Request::new(Method::PUT, url(&address));
    request.headers_mut().insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    let body = serde_json::to_vec(&InitDbRequest { rows }).expect("failed to serialize body");
    *request.body_mut() = Some(Body::from(body));
    let (_, body) = execute(client, request, &address).await;
    let response: InitDbResponse = serde_json::from_str(&body).expect("failed to parse init_db response");
    println!("db initialized, rows={}", response.rows);
}

async fn execute(client: &Client, request: Request, url: &str) -> (Version, String) {
    let response = client.execute(request).await.unwrap_or_else(|err| panic!("failed to call, url={url}, err={err}"));
    let status = response.status();
    let version = response.version();
    let body = response.text().await.unwrap_or_else(|err| panic!("failed to read body, url={url}, err={err}"));
    assert!(status.is_success(), "unexpected status, url={url}, status={status}, body={body}");
    (version, body)
}

/// What this client was asked to do, the `config` part of the result file.
fn config(args: &Args) -> serde_json::Value {
    json!({
        "scenario": args.scenario.as_str(),
        "protocol": "h2c",
        "url": args.url,
        "concurrency": args.concurrency,
        "threads": args.threads,
        "values": args.values,
        "rows": args.rows,
        "warmup": args.warmup.as_secs(),
        "duration": args.duration.as_secs(),
    })
}

fn cpu_pct(cpu_us: u64, elapsed: Duration, cores: usize) -> f64 {
    let available = elapsed.as_secs_f64() * 1_000_000.0 * cores.max(1) as f64;
    if available > 0.0 { cpu_us as f64 * 100.0 / available } else { 0.0 }
}

fn url(value: &str) -> Url {
    Url::parse(value).unwrap_or_else(|err| panic!("invalid url, url={value}, err={err}"))
}
