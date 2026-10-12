use std::sync::Arc;
use std::sync::LazyLock;

use framework::appender::TraceAppender;
use framework::exception::Exception;
use framework::system::DefaultEnv;
use framework::system::System;
use framework::web::request::Request;
use framework::web::response::Response;
use framework::web::router::Router;
use framework::web::server::HttpServer;
use framework::web::server::HttpServerConfig;
use framework_db::Database;
use http_test_server::BenchmarkService;
use http_test_server::DbInsertRequest;
use http_test_server::DbInsertResponse;
use http_test_server::DbSelectResponse;
use http_test_server::GetRequest;
use http_test_server::GetResponse;
use http_test_server::InitDbRequest;
use http_test_server::InitDbResponse;
use http_test_server::PostRequest;
use http_test_server::PostResponse;
use http_test_server::info::MachineInfo;
use http_test_server::info::ProcessUsage;
use http_test_server::info::ServerInfo;

mod db;

/// The target under test, a framework app with nothing but the http server wired up.
#[tokio::main]
async fn main() {
    let mut system = System::init(env!("CARGO_PKG_NAME"), DefaultEnv).await;

    // collected before serving, so the shell commands behind it never run during a measured phase
    LazyLock::force(&MACHINE);

    // the pool connects lazily, so the non db scenarios run without postgres
    let database = db::database();
    system.add_metrics(database.metrics());
    let app = Router::new()
        .merge(
            Router::new()
                .state(Arc::new(()))
                .get("/benchmark/get", get_benchmark)
                .post("/benchmark/post", post_benchmark),
        )
        .merge(BenchmarkService::route(Arc::new(BenchmarkServiceImpl { database })));

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

static MACHINE: LazyLock<MachineInfo> = LazyLock::new(MachineInfo::collect);

// controllers do no work on purpose, what is measured is everything around them
async fn get_benchmark(_state: Arc<()>, request: Request) -> Result<Response, Exception> {
    let request: GetRequest = request.query()?;
    Response::json(&GetResponse::new(&request))
}

async fn post_benchmark(_state: Arc<()>, mut request: Request) -> Result<Response, Exception> {
    let request: PostRequest = request.json().await?;
    Response::json(&PostResponse::new(&request))
}

struct BenchmarkServiceImpl {
    database: Database,
}

impl BenchmarkService for BenchmarkServiceImpl {
    async fn get(&self, request: GetRequest) -> Result<GetResponse, Exception> {
        Ok(GetResponse::new(&request))
    }

    async fn post(&self, request: PostRequest) -> Result<PostResponse, Exception> {
        Ok(PostResponse::new(&request))
    }

    async fn info(&self) -> Result<ServerInfo, Exception> {
        Ok(ServerInfo {
            machine: MACHINE.clone(),
            threads: tokio::runtime::Handle::current().metrics().num_workers(),
            usage: ProcessUsage::current(),
        })
    }

    async fn init_db(&self, request: InitDbRequest) -> Result<InitDbResponse, Exception> {
        db::init_db(&self.database, request).await
    }

    async fn db_select(&self, request: GetRequest) -> Result<DbSelectResponse, Exception> {
        db::select(&self.database, request).await
    }

    async fn db_insert_ignore(&self, request: DbInsertRequest) -> Result<DbInsertResponse, Exception> {
        db::insert_ignore(&self.database, request).await
    }
}
