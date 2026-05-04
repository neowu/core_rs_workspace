use framework::exception::Exception;
use framework::http::HttpClient;
use framework::http::HttpMethod::GET;
use framework::http::HttpMethod::POST;
use framework::http::HttpRequest;
use framework::http::header;
use framework::log;
use framework::log::appender::ConsoleAppender;
use tokio_stream::StreamExt;
use tracing::debug;
use tracing::warn;

#[tokio::main]
async fn main() {
    log::init_with_action(ConsoleAppender);

    log::start_action("test_http_client", None, async {
        // test_http().await
        test_sse().await
    })
    .await;
}

#[allow(unused)]
async fn test_http() -> Result<(), Exception> {
    let http_client = HttpClient::default();
    let mut request = HttpRequest::new(POST, "https://localhost:8443");
    request.body("{some json}".to_owned(), "application/json".to_owned());
    request.headers.insert(header::USER_AGENT, "Rust".to_string());

    let _response = http_client.execute(request).await?;

    // let mut lines = response.lines();
    // while let Some(line) = lines.next().await {
    //     let line = line?;
    //     println!("line={line}");
    // }
    debug!(http_client_hello = 1, "stats");
    warn!("test");
    Ok(())
}

#[allow(unused)]
async fn test_sse() -> Result<(), Exception> {
    let http_client = HttpClient::default();
    let request = HttpRequest::new(GET, "https://localhost:8443/sse");
    let mut source = http_client.sse(request).await?;
    while let Some(result) = source.next().await {
        let event = result?;
        println!("event => {event:?}");
    }

    warn!("test");

    Ok(())
}
