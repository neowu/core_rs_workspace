use std::time::Duration;

use demo::AppConfig;
use framework::exception::Exception;
use framework::http::HttpClient;
use framework::http::HttpClientConfig;
use framework::http::HttpRequest;
use framework::http::Method;
use framework::http::StreamExt;
use framework::load_config;
use framework::log;
use framework::log::appender::ConsoleAppender;
use framework::log::appender::GCloudAppender;
use framework::stats;
use framework::system::System;
use framework::warn;

#[tokio::main]
async fn main() -> Result<(), Exception> {
    let config: AppConfig = load_config!("assets/conf.json");

    let mut system = System::init(env!("CARGO_PKG_NAME"));
    match config.log_appender.as_str() {
        "console" => system.start_action_logger(ConsoleAppender),
        "gcloud" => system.start_action_logger(GCloudAppender),
        value => panic!("unknown appender, value={value}"),
    }

    let _result = log::action("test_http_client", None, async {
        test_http().await
        // test_sse().await
    })
    .await;

    system.wait().await;
    system.shutdown(Duration::from_secs(15)).await
}

#[allow(unused)]
async fn test_http() -> Result<(), Exception> {
    let http_client = HttpClient::new(HttpClientConfig::default());
    let mut request = HttpRequest::new(Method::GET, "http://localhost:8080/504");
    // request.body("{some json}".to_owned(), "application/json".to_owned());
    // request.headers.insert(header::USER_AGENT, "Rust".to_string());

    let _response = http_client.execute(request).await?;

    // let mut lines = response.lines();
    // while let Some(line) = lines.next().await {
    //     let line = line?;
    //     println!("line={line}");
    // }
    stats!(http_client_hello = 1);
    warn!(error_code = "TRIGGER", "test");
    Ok(())
}

#[allow(unused)]
async fn test_sse() -> Result<(), Exception> {
    let http_client = HttpClient::new(HttpClientConfig::default());
    let request = HttpRequest::new(Method::GET, "https://localhost:8443/sse");
    let mut source = http_client.sse(request).await?;
    while let Some(result) = source.next().await {
        let event = result?;
        println!("event => {event:?}");
    }

    warn!(error_code = "TRIGGER", "test");

    Ok(())
}
