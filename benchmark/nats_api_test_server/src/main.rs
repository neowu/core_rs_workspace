use std::env;
use std::sync::Arc;

use framework::appender::TraceAppender;
use framework::exception::Exception;
use framework::system::DefaultEnv;
use framework::system::System;
use framework_nats::service::ServiceConfig;
use nats_api_test_server::BenchmarkService;
use nats_api_test_server::GetRequest;
use nats_api_test_server::GetResponse;
use nats_api_test_server::PostRequest;
use nats_api_test_server::PostResponse;

#[cfg(feature = "alloc_stats")]
#[global_allocator]
static ALLOCATOR: harness::alloc_stats::Tracking = harness::alloc_stats::Tracking;

const DEFAULT_URL: &str = "nats.test:4222";
// the client's concurrency is what a run varies, so the service semaphore is set well above it --
// otherwise a run would silently measure the semaphore instead of the framework
const DEFAULT_MAX_CONCURRENCY: usize = 4096;

/// The target under test, a framework app with nothing but the nats service wired up.
#[tokio::main]
async fn main() {
    let mut system = System::init(env!("CARGO_PKG_NAME"), DefaultEnv).await;

    let url = env::var("NATS_URL").unwrap_or_else(|_| DEFAULT_URL.to_owned());
    let nats_client = framework_nats::connect(&url).await;

    let config = ServiceConfig { max_concurrency: max_concurrency() };
    let service = BenchmarkService::service(nats_client, Arc::new(BenchmarkServiceImpl), config);
    system.add_metrics(service.metrics());

    // TraceAppender keeps action construction and the channel send, real framework cost, but writes
    // nothing per request, so appender output never becomes the bottleneck under load
    let system = system.start_logger(TraceAppender);
    system.start_service(|token| service.start(token));

    system.wait().await;
    system.shutdown_logger().await;

    report_alloc_stats();
}

fn max_concurrency() -> usize {
    env::var("MAX_CONCURRENCY").map_or(DEFAULT_MAX_CONCURRENCY, |value| value.parse().expect("invalid concurrency"))
}

#[cfg(feature = "alloc_stats")]
fn report_alloc_stats() {
    let (allocs, bytes, live) = harness::alloc_stats::snapshot();
    framework::console!("alloc_stats: allocs={allocs}, bytes={bytes}, live={live}");
}

#[cfg(not(feature = "alloc_stats"))]
const fn report_alloc_stats() {}

struct BenchmarkServiceImpl;

// handlers do no work on purpose, what is measured is everything around them
impl BenchmarkService for BenchmarkServiceImpl {
    async fn get(&self, request: GetRequest) -> Result<GetResponse, Exception> {
        Ok(GetResponse::new(&request))
    }

    async fn post(&self, request: PostRequest) -> Result<PostResponse, Exception> {
        Ok(PostResponse::new(&request))
    }
}
