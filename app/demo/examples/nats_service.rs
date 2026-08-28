use std::sync::Arc;
use std::time::Duration;

use demo::AppConfig;
use framework::exception;
use framework::exception::Exception;
use framework::load_config;
use framework::log::ConsoleAppender;
use framework::log::GCloudAppender;
use framework::metrics::MetricsCollector;
use framework::spawn_action;
use framework::system::System;
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
pub async fn main() -> Result<(), Exception> {
    let config: AppConfig = load_config!("assets/conf.json");

    let mut system = System::init(env!("CARGO_PKG_NAME"));
    match config.log_appender.as_str() {
        "console" => system.start_action_logger(ConsoleAppender),
        "gcloud" => system.start_action_logger(GCloudAppender),
        value => panic!("unknown appender, value={value}"),
    }

    let mut collector = MetricsCollector::new();
    let nats_client = framework_nats::connect("dev.internal:4222").await;

    let service =
        GreetingService::service(nats_client.clone(), Arc::new(GreetingServiceImpl), ServiceConfig::default());
    let client = GreetingServiceClient::new(nats_client);
    collector.add(service.metrics());

    system.start_service(|token| service.start(token));
    system.start_metrics_collector(collector, ConsoleAppender);

    spawn_action!("client", async move {
        let response = client.greet(GreetRequest { name: "world".to_owned() }).await?;
        println!("greet response: {response:?}");
        let result = client.fail().await;
        println!("fail result: {result:?}");
        Ok(())
    });

    system.wait().await;
    system.shutdown(Duration::from_secs(15)).await
}
