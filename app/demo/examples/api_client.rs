use std::time::Duration;

use demo::AppConfig;
use demo::user::CreateUserRequest;
use demo::user::GetUserByNameRequest;
use demo::user::UpdateUserRequest;
use demo::user::UserService;
use demo::user::UserServiceClient;
use framework::appender::ConsoleAppender;
use framework::appender::GCloudAppender;
use framework::http::HttpClient;
use framework::http::HttpClientConfig;
use framework::load_config;
use framework::log;
use framework::spawn_action;
use framework::system::System;
use framework::task::start_executor;

#[tokio::main]
async fn main() {
    let config: AppConfig = load_config!("assets/conf.json");

    let system = System::init(env!("CARGO_PKG_NAME"));

    let executor = start_executor();

    let system = match config.log_appender.as_str() {
        "console" => system.start_logger(ConsoleAppender),
        "gcloud" => system.start_logger(GCloudAppender),
        value => panic!("unknown appender, value={value}"),
    };

    let client =
        UserServiceClient::new(HttpClient::new(HttpClientConfig::internal_only()), "http://localhost:8080".to_owned());

    spawn_action!("client", async move {
        let user_id = client.create(CreateUserRequest { name: "".to_owned(), rating: None }).await?;

        client.update(UpdateUserRequest { id: user_id, rating: Some(1), tags: Some(vec!["tag1".to_owned()]) }).await?;

        let _user = client.get_by_name(GetUserByNameRequest { name: "user_3".to_owned() }).await?;

        log!("trigger");

        Ok(())
    });

    system.wait().await;
    executor.shutdown(Duration::from_secs(15)).await;
    system.shutdown_logger().await;
}
