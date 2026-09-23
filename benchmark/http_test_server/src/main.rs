use std::sync::Arc;

use axum::Router;
use framework::appender::TraceAppender;
use framework::exception::Exception;
use framework::system::DefaultEnv;
use framework::system::System;
use framework::web::body::Json;
use framework::web::body::Query;
use framework::web::route::get;
use framework::web::route::post;
use framework::web::server::HttpServer;
use framework::web::server::HttpServerConfig;
use http_test_server::BenchmarkService;
use http_test_server::GetRequest;
use http_test_server::GetResponse;
use http_test_server::PostRequest;
use http_test_server::PostResponse;

/// The target under test, a framework app with nothing but the http server wired up.
#[tokio::main]
async fn main() {
    let mut system = System::init(env!("CARGO_PKG_NAME"), DefaultEnv).await;

    let app = Router::new();
    let app = app.route("/benchmark/get", get(get_benchmark));
    let app = app.route("/benchmark/post", post(post_benchmark));
    let app = app.merge(BenchmarkService::route(Arc::new(BenchmarkServiceImpl)));

    // the default binds 0.0.0.0:8080, a benchmark host keeps that port free
    let http_server = HttpServer::new(HttpServerConfig::default());
    system.add_metrics(http_server.metrics());

    // TraceAppender keeps action construction and the channel send, real framework cost, but writes
    // nothing per request, so appender output never becomes the bottleneck under load
    let system = system.start_logger(TraceAppender);
    system.start_service(|token| http_server.start(app, token));

    system.wait().await;
    system.shutdown_logger().await;
}

// controllers do no work on purpose, what is measured is everything around them
async fn get_benchmark(Query(request): Query<GetRequest>) -> Json<GetResponse> {
    Json(GetResponse::new(&request))
}

async fn post_benchmark(Json(request): Json<PostRequest>) -> Json<PostResponse> {
    Json(PostResponse::new(&request))
}

struct BenchmarkServiceImpl;

impl BenchmarkService for BenchmarkServiceImpl {
    async fn get(&self, request: GetRequest) -> Result<GetResponse, Exception> {
        Ok(GetResponse::new(&request))
    }

    async fn post(&self, request: PostRequest) -> Result<PostResponse, Exception> {
        Ok(PostResponse::new(&request))
    }
}
