use std::fs::write;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use framework::http::HeaderName;
use framework::http::HeaderValue;
use framework::http::HttpClient;
use framework::http::HttpClientConfig;
use framework::http::HttpRequest;
use framework::http::Method;
use framework::http::RetryConfig;
use framework::http::header::ACCEPT;
use framework::http::header::ACCEPT_ENCODING;
use framework::http::header::ACCEPT_LANGUAGE;
use framework::http::header::USER_AGENT;
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
use serde_json::json;

use crate::args::Args;
use crate::args::Scenario;
use crate::stats::Recorder;
use crate::stats::Summary;

mod args;
mod stats;

const ID: i64 = 7;

/// Sent with every scenario request, what a browser behind a gateway sends: the framework's `client`
/// and `ref-id` plus a desktop Safari navigation, so the server's header logging does real work.
const HEADERS: [(HeaderName, &str); 10] = [
    (HeaderName::from_static("client"), "benchmark"),
    (HeaderName::from_static("ref-id"), "benchmark-ref-id"),
    (ACCEPT, "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"),
    (ACCEPT_ENCODING, "gzip, deflate, br, zstd"),
    (ACCEPT_LANGUAGE, "en-US,en;q=0.9"),
    (HeaderName::from_static("priority"), "u=0, i"),
    (HeaderName::from_static("sec-fetch-dest"), "document"),
    (HeaderName::from_static("sec-fetch-mode"), "navigate"),
    (HeaderName::from_static("sec-fetch-site"), "none"),
    (
        USER_AGENT,
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/27.0.1 Safari/605.1.15",
    ),
];

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
    request: HttpRequest,
}

impl Target {
    fn new(args: &Args) -> Target {
        let scenario = args.scenario;
        let path = scenario.path();
        let mut request = if scenario.is_post() {
            let body = match scenario {
                // one id past the seeded rows: the first insert lands, every later one conflicts
                Scenario::DbInsertIgnore => serde_json::to_string(&DbInsertRequest {
                    id: args.rows + 1,
                    name: "benchmark".to_owned(),
                    amount: 100,
                }),
                _ => serde_json::to_string(&PostRequest {
                    id: ID,
                    name: "benchmark".to_owned(),
                    values: vec![1; args.values],
                }),
            }
            .expect("failed to serialize body");
            let mut request = request(Method::POST, &format!("{}{path}", args.url));
            request.body(body, "application/json");
            request
        } else {
            request(Method::GET, &format!("{}{path}?id={ID}", args.url))
        };
        request.headers.extend(HEADERS.into_iter().map(|(name, value)| (name, HeaderValue::from_static(value))));
        Target { scenario, request }
    }
}

async fn run(args: Args) {
    // a client holds one h2 connection per host, so each connection is its own client
    let clients: Vec<Arc<HttpClient>> = (0..args.connections).map(|_| Arc::new(client())).collect();
    let client = &clients[0];

    if args.scenario.is_db() {
        init_db(client, &args.url, args.rows).await;
    }

    let target = Target::new(&args);
    verify(client, &target, &args).await;

    // opens each client's connection up front, a burst of requests on an empty pool would each connect
    for client in &clients[1..] {
        execute(client, target.request.clone()).await;
    }

    if !args.warmup.is_zero() {
        load(&clients, &target, args.concurrency, args.warmup).await;
        println!("warmup done");
    }

    // sampled around the measured phase only, so server cpu per request excludes the warmup
    let before = server_info(client, &args.url).await;
    let client_before = ProcessUsage::current();
    let summary = load(&clients, &target, args.concurrency, args.duration).await;
    let client_after = ProcessUsage::current();
    let after = server_info(client, &args.url).await;

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

async fn server_info(client: &HttpClient, base_url: &str) -> ServerInfo {
    let address = format!("{base_url}/benchmark/info");
    let body = execute(client, request(Method::GET, &address)).await;
    serde_json::from_str(&body).unwrap_or_else(|err| panic!("failed to parse server info, url={address}, err={err}"))
}

/// h2c as configured for internal calls (HttpClientConfig::internal_only), one kept alive connection
/// carrying every request of this client as a multiplexed stream. Without an action context the
/// client's logging is a no-op. No retry, so every failure is counted.
fn client() -> HttpClient {
    HttpClient::new(HttpClientConfig {
        timeout: Duration::from_secs(10),
        retry: RetryConfig { max_attempts: 1, interval: Duration::ZERO },
        ..HttpClientConfig::internal_only()
    })
}

/// Closed loop: every worker holds one in flight request and sends the next as soon as the previous
/// one resolves, so `concurrency` is the number of open h2 streams and the server's own pace sets
/// the rate. Workers take the connections round robin, so each carries an even share of the streams.
async fn load(clients: &[Arc<HttpClient>], target: &Target, concurrency: usize, duration: Duration) -> Summary {
    let start = Instant::now();
    let deadline = start + duration;

    let mut handles = Vec::with_capacity(concurrency);
    for i in 0..concurrency {
        let client = Arc::clone(&clients[i % clients.len()]);
        handles.push(tokio::spawn(worker(client, target.clone(), deadline)));
    }

    let mut recorders = Vec::with_capacity(concurrency);
    for handle in handles {
        recorders.push(handle.await.expect("worker cannot panic"));
    }

    Summary::merge(recorders, start.elapsed())
}

async fn worker(client: Arc<HttpClient>, target: Target, deadline: Instant) -> Recorder {
    let mut recorder = Recorder::default();
    loop {
        let start = Instant::now();
        if start >= deadline {
            break;
        }
        // execute reads the whole body before it returns
        match client.execute(target.request.clone()).await {
            Ok(response) if (200..300).contains(&response.status) => {
                recorder.record(start.elapsed().as_nanos() as u64);
            }
            Ok(_) => recorder.failed += 1,
            Err(_) => recorder.errors += 1,
        }
    }
    recorder
}

/// Sends one request of the selected scenario and parses the response, so a wrong url or a broken
/// server fails here instead of producing a fast benchmark of 404s. The measured loop never parses.
/// The client speaks h2c with prior knowledge and never falls back to http/1.1, a server without h2c
/// fails here.
async fn verify(client: &HttpClient, target: &Target, args: &Args) {
    let body = execute(client, target.request.clone()).await;

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

    println!("verified, scenario={}, protocol=h2c", target.scenario.as_str());
}

/// Drops and recreates the table, so every db run starts from the same state.
async fn init_db(client: &HttpClient, base_url: &str, rows: i64) {
    let mut request = request(Method::PUT, &format!("{base_url}/benchmark/init_db"));
    request.body(serde_json::to_string(&InitDbRequest { rows }).expect("failed to serialize body"), "application/json");
    let body = execute(client, request).await;
    let response: InitDbResponse = serde_json::from_str(&body).expect("failed to parse init_db response");
    println!("db initialized, rows={}", response.rows);
}

async fn execute(client: &HttpClient, request: HttpRequest) -> String {
    let response = client.execute(request).await.unwrap_or_else(|err| panic!("failed to call, err={err}"));
    let status = response.status;
    let body = String::from_utf8_lossy(&response.body).into_owned();
    assert!((200..300).contains(&status), "unexpected status, status={status}, body={body}");
    body
}

/// What this client was asked to do, the `config` part of the result file.
fn config(args: &Args) -> serde_json::Value {
    json!({
        "scenario": args.scenario.as_str(),
        "protocol": "h2c",
        "url": args.url,
        "concurrency": args.concurrency,
        "connections": args.connections,
        "threads": args.threads,
        "values": args.values,
        "rows": args.rows,
        "headers": HEADERS.iter().map(|(name, _)| name.as_str()).collect::<Vec<_>>(),
        "warmup": args.warmup.as_secs(),
        "duration": args.duration.as_secs(),
    })
}

fn cpu_pct(cpu_us: u64, elapsed: Duration, cores: usize) -> f64 {
    let available = elapsed.as_secs_f64() * 1_000_000.0 * cores.max(1) as f64;
    if available > 0.0 { cpu_us as f64 * 100.0 / available } else { 0.0 }
}

fn request(method: Method, url: &str) -> HttpRequest {
    HttpRequest::new(method, url).unwrap_or_else(|err| panic!("invalid url, url={url}, err={err}"))
}
