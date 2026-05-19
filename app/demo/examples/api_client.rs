use demo::user::CreateUserRequest;
use demo::user::GetUserByNameRequest;
use demo::user::UpdateUserRequest;
use demo::user::UserService;
use demo::user::user_service;
use framework::exception::Exception;
use framework::http::HttpClient;
use framework::http::HttpClientConfig;
use framework::log;
use framework::log::appender::ConsoleAppender;
use framework::task;
use tracing::warn;

#[tokio::main]
async fn main() -> Result<(), Exception> {
    log::init_with_action(ConsoleAppender);

    let client = user_service::client(HttpClient::new(HttpClientConfig::internal_only()), "http://localhost:8080");

    task::spawn_action("client", async move {
        let user_id = client.create(CreateUserRequest { name: "".to_owned(), rating: None }).await?;

        client.update(UpdateUserRequest { id: user_id, rating: Some(1), tags: Some(vec!["tag1".to_owned()]) }).await?;

        let _user = client.get_by_name(GetUserByNameRequest { name: "user_2".to_owned() }).await?;

        warn!("trigger");

        Ok(())
    });

    task::shutdown().await;

    Ok(())
}
