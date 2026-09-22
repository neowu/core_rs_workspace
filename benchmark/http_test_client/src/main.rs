use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use harness::stats;
use harness::stats::Recorder;
use harness::stats::Summary;
use http_test_server::GetResponse;
use http_test_server::PostRequest;
use http_test_server::PostResponse;
use reqwest::Body;
use reqwest::Client;
use reqwest::Method;
use reqwest::Request;
use reqwest::Url;
use reqwest::Version;
use reqwest::header::CONTENT_TYPE;
use reqwest::header::HeaderValue;

use crate::args::Args;
use crate::args::Scenario;

mod args;

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
            let request = PostRequest { id: ID, name: "benchmark".to_owned(), values: vec![1; args.values] };
            let body = serde_json::to_vec(&request).expect("failed to serialize body");
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

    let target = Target::new(&args);
    verify(&client, &target, args.values).await;

    println!(
        "scenario={}, url={}, protocol=h2c, concurrency={}, threads={}, warmup={}s, duration={}s",
        args.scenario.as_str(),
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
async fn verify(client: &Client, target: &Target, values: usize) {
    let url = target.url.as_str();
    let (version, body) = execute(client, target.request(), url).await;

    // a silent fallback to http/1.1 would quietly benchmark a protocol nobody runs internally
    assert_eq!(version, Version::HTTP_2, "server must speak h2c, url={url}");

    if target.scenario.is_post() {
        let response: PostResponse = serde_json::from_str(&body).expect("failed to parse post response");
        assert_eq!(response.id, ID, "post endpoint must echo the id");
        assert_eq!(response.sum, values as i64, "post endpoint must sum the values");
    } else {
        let response: GetResponse = serde_json::from_str(&body).expect("failed to parse get response");
        assert_eq!(response.id, ID, "get endpoint must echo the id");
    }

    println!("verified, url={url}, protocol=h2c");
}

async fn execute(client: &Client, request: Request, url: &str) -> (Version, String) {
    let response = client.execute(request).await.unwrap_or_else(|err| panic!("failed to call, url={url}, err={err}"));
    let status = response.status();
    let version = response.version();
    let body = response.text().await.unwrap_or_else(|err| panic!("failed to read body, url={url}, err={err}"));
    assert!(status.is_success(), "unexpected status, url={url}, status={status}, body={body}");
    (version, body)
}

/// What this client was asked to do, the prefix of the record line `run_http_test.sh` folds into
/// the report.
fn config(args: &Args) -> String {
    format!(
        "scenario={} protocol=h2c concurrency={} threads={} values={} warmup={} duration={}",
        args.scenario.as_str(),
        args.concurrency,
        args.threads,
        args.values,
        args.warmup.as_secs(),
        args.duration.as_secs()
    )
}

fn url(value: &str) -> Url {
    Url::parse(value).unwrap_or_else(|err| panic!("invalid url, url={value}, err={err}"))
}
