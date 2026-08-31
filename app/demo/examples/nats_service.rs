use std::sync::Arc;
use std::time::Duration;

use demo::AppConfig;
use framework::appender::ConsoleAppender;
use framework::appender::GCloudAppender;
use framework::exception;
use framework::exception::Exception;
use framework::load_config;
use framework::spawn_action;
use framework::system::System;
use framework::task::start_executor;
use framework_macro::nats_api;
use framework_nats::service::ServiceConfig;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Debug)]
pub struct GreetRequest {
    pub name: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GreetResponse {
    pub greeting: String,
}

#[nats_api]
pub trait GreetingService {
    #[subject = "api.demo.greet"]
    async fn greet(&self, request: GreetRequest) -> Result<GreetResponse, Exception>;

    #[subject = "api.demo.fail"]
    async fn fail(&self) -> Result<(), Exception>;
}

struct GreetingServiceImpl;

impl GreetingService for GreetingServiceImpl {
    async fn greet(&self, request: GreetRequest) -> Result<GreetResponse, Exception> {
        Ok(GreetResponse { greeting: format!("hello, {}", request.name) })
    }

    async fn fail(&self) -> Result<(), Exception> {
        Err(exception!("expected failure", code = "DEMO_FAILURE"))
    }
}

#[tokio::main]
pub async fn main() {
    let config: AppConfig = load_config!("assets/conf.json");

    let mut system = System::init(env!("CARGO_PKG_NAME"));
    let nats_client = framework_nats::connect("dev.internal:4222").await;

    let executor = start_executor();
    system.add_metrics(executor.metrics());

    let service =
        GreetingService::service(nats_client.clone(), Arc::new(GreetingServiceImpl), ServiceConfig::default());
    let client = GreetingServiceClient::new(nats_client);
    system.add_metrics(service.metrics());

    let system = match config.log_appender.as_str() {
        "console" => system.start_logger(ConsoleAppender),
        "gcloud" => system.start_logger(GCloudAppender),
        value => panic!("unknown appender, value={value}"),
    };

    system.start_service(|token| service.start(token));

    spawn_action!("client", async move {
        let response = client.greet(GreetRequest { name: "world".to_owned() }).await?;
        println!("greet response: {response:?}");
        let result = client.fail().await;
        println!("fail result: {result:?}");
        Ok(())
    });

    system.wait().await;
    executor.shutdown(Duration::from_secs(15)).await;
    system.shutdown_logger().await;
}
