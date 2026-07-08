use std::sync::Arc;

use demo::AppConfig;
use framework::exception;
use framework::exception::Exception;
use framework::load_config;
use framework::log;
use framework::spawn_action;
use framework::system::System;
use framework_macro::nats_api;
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
    log::init(&config.log_appender, env!("CARGO_PKG_NAME"));

    let nats_client = framework_nats::connect("dev.internal:4222".to_owned()).await;

    let service = greeting_service::service(nats_client.clone(), Arc::new(GreetingServiceImpl));
    let client = greeting_service::client(nats_client, env!("CARGO_BIN_NAME"));

    let mut system = System::new();
    system.spawn(service.start(system.shutdown_signal()));

    spawn_action!("client", async move {
        let response = client.greet(GreetRequest { name: "world".to_owned() }).await?;
        println!("greet response: {response:?}");
        let result = client.fail().await;
        println!("fail result: {result:?}");
        Ok(())
    });

    system.wait().await;
    Ok(())
}
