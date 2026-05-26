use framework::exception::Exception;
use framework::http::HttpClient;
use framework::http::HttpClientConfig;
use framework::http::HttpRequest;
use framework::http::Method;
use framework::http::StreamExt;
use framework::log;
use framework::stats;
use tracing::warn;

#[tokio::main]
async fn main() -> Result<(), Exception> {
    log::init();
    log::init_action_log_appender("console", env!("CARGO_BIN_NAME"))?;

    let _ = log::start_action("test_http_client", None, async {
        test_http().await
        // test_sse().await
    })
    .await;

    Ok(())
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
    warn!("test");
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

    warn!("test");

    Ok(())
}
