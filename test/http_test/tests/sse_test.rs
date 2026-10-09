use std::future::pending;
use std::sync::Arc;
use std::time::Duration;

use framework::api::ErrorResponse;
use framework::exception;
use framework::exception::Exception;
use framework::exception::error_code;
use framework::log::Severity;
use framework::web::request::Request;
use framework::web::response::Response;
use framework::web::router::Router;
use framework::web::sse::Event;
use framework_macro::integration_test;
use http_test::TestServer;
use reqwest::Client;
use reqwest::StatusCode;
use reqwest::header;
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::sleep;
use tokio::time::timeout;

struct AppState {
    signal: UnboundedSender<&'static str>,
}

// signals when the sse handler returns or is dropped
struct Finished(UnboundedSender<&'static str>, &'static str);

impl Drop for Finished {
    fn drop(&mut self) {
        let _sent = self.0.send(self.1);
    }
}

async fn events(_state: Arc<AppState>, request: Request) -> Result<Response, Exception> {
    let last_event_id = request.header("last-event-id").unwrap_or_default().to_owned();
    Ok(Response::sse(move |channel| async move {
        channel.send(&Event::new(&format!("a\nb{last_event_id}")).id("1")).await;
        channel.send(&Event::json(&vec!["x"])?.event("list")).await;
        Ok(())
    }))
}

async fn forbidden(_state: Arc<AppState>, _request: Request) -> Result<Response, Exception> {
    Err(exception!("not allowed", severity = Severity::Warn, code = error_code::FORBIDDEN))
}

async fn fail(_state: Arc<AppState>, _request: Request) -> Result<Response, Exception> {
    Ok(Response::sse(|channel| async move {
        channel.send(&Event::new("before")).await;
        Err(exception!("handler failed"))
    }))
}

async fn ticks(state: Arc<AppState>, _request: Request) -> Result<Response, Exception> {
    Ok(Response::sse(move |channel| async move {
        let _finished = Finished(state.signal.clone(), "ticks");
        while channel.send(&Event::new("tick")).await {
            sleep(Duration::from_millis(10)).await;
        }
        Ok(())
    }))
}

async fn hold(state: Arc<AppState>, _request: Request) -> Result<Response, Exception> {
    Ok(Response::sse(move |channel| async move {
        let _finished = Finished(state.signal.clone(), "hold");
        channel.send(&Event::new("hello")).await;
        pending::<()>().await;
        Ok(())
    }))
}

async fn silent(state: Arc<AppState>, _request: Request) -> Result<Response, Exception> {
    Ok(Response::sse(move |channel| async move {
        let _finished = Finished(state.signal.clone(), "silent");
        // the stream ends once every channel is dropped
        let _channel = channel;
        pending::<()>().await;
        Ok(())
    }))
}

async fn next_signal(signals: &mut UnboundedReceiver<&'static str>) -> &'static str {
    timeout(Duration::from_secs(2), signals.recv()).await.expect("no signal").expect("sender dropped")
}

fn assert_retry(body: &str, prefix: &str) {
    let retry = body.strip_prefix(prefix).and_then(|v| v.strip_prefix("retry: ")).and_then(|v| v.strip_suffix("\n\n"));
    let retry: u32 = retry.and_then(|v| v.parse().ok()).unwrap_or_else(|| panic!("unexpected body, body={body:?}"));
    assert!((1000..5000).contains(&retry));
}

#[integration_test]
async fn sse() -> Result<(), Exception> {
    let (signal, mut signals) = mpsc::unbounded_channel();
    let router = Router::new()
        .state(Arc::new(AppState { signal }))
        .get("/events", events)
        .get("/forbidden", forbidden)
        .get("/fail", fail)
        .get("/ticks", ticks)
        .get("/hold", hold)
        .get("/silent", silent);
    let mut server = TestServer::start(router).await;

    for client in [&server.http1, &server.h2c] {
        let response = client.get(server.url("/events")).header("last-event-id", "0").send().await?;
        assert_eq!(response.status(), 200);
        assert_eq!(response.headers()[header::CONTENT_TYPE], "text/event-stream");
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-cache");
        assert!(response.headers().get(header::CONTENT_LENGTH).is_none());
        // the stream ends when the handler returns
        assert_eq!(response.text().await?, "data: a\ndata: b0\nid: 1\n\ndata: [\"x\"]\nevent: list\n\n");

        // the controller rejects before the stream starts, a normal error response
        let response = client.get(server.url("/forbidden")).send().await?;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let error: ErrorResponse = response.json().await?;
        assert_eq!(error.code.as_deref(), Some(error_code::FORBIDDEN));

        // an error in the sse handler ends the stream normally, it is logged with the sse action
        let response = client.get(server.url("/fail")).send().await?;
        assert_eq!(response.status(), 200);
        assert_eq!(response.text().await?, "data: before\n\n");

        // HEAD doesn't run the sse handler, it would signal "hold" before "ticks" below
        let response = client.head(server.url("/hold")).send().await?;
        assert_eq!(response.status(), 200);
        assert_eq!(response.headers()[header::CONTENT_TYPE], "text/event-stream");
        assert_eq!(response.text().await?, "");

        // client disconnect drops the handler
        let mut response = client.get(server.url("/ticks")).send().await?;
        let chunk = response.chunk().await?.expect("expect sse event");
        assert!(chunk.starts_with(b"data: tick\n\n"));
        drop(response);
        assert_eq!(next_signal(&mut signals).await, "ticks");
    }

    let send = |client: &Client, path: &str| client.get(server.url(path)).send();
    let hold_responses = [send(&server.http1, "/hold").await?, send(&server.h2c, "/hold").await?];
    // headers are sent before any event
    let silent_response = send(&server.h2c, "/silent").await?;
    assert_eq!(silent_response.status(), 200);

    // open sse streams are closed on shutdown and don't block graceful drain, handlers are dropped
    server.shutdown().await;
    let mut finished =
        [next_signal(&mut signals).await, next_signal(&mut signals).await, next_signal(&mut signals).await];
    finished.sort_unstable();
    assert_eq!(finished, ["hold", "hold", "silent"]);

    for response in hold_responses {
        assert_retry(&response.text().await?, "data: hello\n\n");
    }
    assert_retry(&silent_response.text().await?, "");
    Ok(())
}
