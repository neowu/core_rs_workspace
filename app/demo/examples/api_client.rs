use std::time::Duration;

use demo::AppConfig;
use demo::user::CreateUserRequest;
use demo::user::GetUserByNameRequest;
use demo::user::UpdateUserRequest;
use demo::user::UserService;
use demo::user::user_service;
use framework::exception::Exception;
use framework::http::HttpClient;
use framework::http::HttpClientConfig;
use framework::load_config;
use framework::log;
use framework::spawn_action;
use framework::task;

#[tokio::main]
async fn main() -> Result<(), Exception> {
    let config: AppConfig = load_config!("assets/conf.json");
    log::init(&config.log_appender, env!("CARGO_PKG_NAME"));

    let client = user_service::client(
        HttpClient::new(HttpClientConfig::internal_only()),
        "http://localhost:8080".to_owned(),
        env!("CARGO_BIN_NAME"),
    );

    spawn_action!("client", async move {
        let user_id = client.create(CreateUserRequest { name: "".to_owned(), rating: None }).await?;

        client.update(UpdateUserRequest { id: user_id, rating: Some(1), tags: Some(vec!["tag1".to_owned()]) }).await?;

        let _user = client.get_by_name(GetUserByNameRequest { name: "user_3".to_owned() }).await?;

        log!("trigger");

        Ok(())
    });

    task::shutdown(Duration::from_secs(15)).await;

    Ok(())
}
