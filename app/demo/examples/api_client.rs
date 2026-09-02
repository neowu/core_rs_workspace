use std::time::Duration;

use demo::user::CreateUserRequest;
use demo::user::GetUserByNameRequest;
use demo::user::UpdateUserRequest;
use demo::user::UserService;
use demo::user::UserServiceClient;
use framework::appender::ConsoleAppender;
use framework::http::HttpClient;
use framework::http::HttpClientConfig;
use framework::log;
use framework::spawn_action;
use framework::system::System;
use framework::task::start_executor;

#[tokio::main]
async fn main() {
    let system = System::init(env!("CARGO_BIN_NAME"));

    let executor = start_executor();

    let system = system.start_logger(ConsoleAppender);

    let client =
        UserServiceClient::new(HttpClient::new(HttpClientConfig::default()), "http://127.0.0.1:8080".to_owned());

    spawn_action!("client", async move {
        log::trace();

        let user_id = client.create(CreateUserRequest { name: "neo".to_owned(), rating: Some(-1) }).await?;

        client.update(UpdateUserRequest { id: user_id, rating: Some(1), tags: Some(vec!["tag1".to_owned()]) }).await?;

        let _user = client.get_by_name(GetUserByNameRequest { name: "user_3".to_owned() }).await?;

        Ok(())
    });

    system.wait().await;
    executor.shutdown(Duration::from_secs(15)).await;
    system.shutdown_logger().await;
}
