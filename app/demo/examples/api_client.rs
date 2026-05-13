use demo::web::CreateUserRequest;
use demo::web::UserService;
use demo::web::user_service;
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

    let client = user_service::client(HttpClient::new(HttpClientConfig::default()), "http://localhost:8080");

    task::spawn_action("client", async move {
        client.create(CreateUserRequest { name: "yes".to_owned() }).await?;
        warn!("client");
        let resp = client.get().await?;
        println!("{resp:?}");
        Ok(())
    });

    task::shutdown().await;

    Ok(())
}
