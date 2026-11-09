use std::future::pending;
use std::sync::Arc;
use std::time::Duration;

use framework::api::ErrorResponse;
use framework::exception;
use framework::exception::Exception;
use framework::exception::error_code;
use framework::http::EventSource;
use framework::http::HeaderName;
use framework::http::HttpRequest;
use framework::http::Method;
use framework::http::header;
use framework::json;
use framework::log::Severity;
use framework::web::Event;
use framework::web::request::Request;
use framework::web::response::Response;
use framework::web::router::Router;
use framework_macro::integration_test;
use futures::TryStreamExt as _;
use http_test::TestServer;
use http_test::execute;
use http_test::http_client;
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
        let event = Event { id: Some("1".to_owned()), event_type: None, data: format!("a\nb{last_event_id}") };
        channel.send(&event).await;
        let mut event = Event::json(&vec!["x"])?;
        event.event_type = Some("list".to_owned());
        channel.send(&event).await;
        Ok(())
    }))
}

async fn forbidden(_state: Arc<AppState>, _request: Request) -> Result<Response, Exception> {
    Err(exception!("not allowed", severity = Severity::Warn, code = error_code::FORBIDDEN))
}

async fn fail(_state: Arc<AppState>, _request: Request) -> Result<Response, Exception> {
    Ok(Response::sse(|channel| async move {
        channel.send(&Event::new("before".to_owned())).await;
        Err(exception!("handler failed"))
    }))
}

async fn ticks(state: Arc<AppState>, _request: Request) -> Result<Response, Exception> {
    Ok(Response::sse(move |channel| async move {
        let _finished = Finished(state.signal.clone(), "ticks");
        while channel.send(&Event::new("tick".to_owned())).await {
            sleep(Duration::from_millis(10)).await;
        }
        Ok(())
    }))
}

async fn hold(state: Arc<AppState>, _request: Request) -> Result<Response, Exception> {
    Ok(Response::sse(move |channel| async move {
        let _finished = Finished(state.signal.clone(), "hold");
        channel.send(&Event::new("hello".to_owned())).await;
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

// reads the stream to the end, which must end with a retry frame
async fn assert_retry(mut source: EventSource, expected: &[&str]) -> Result<(), Exception> {
    let mut events = vec![];
    while let Some(event) = source.try_next().await? {
        events.push(event.data);
    }
    assert_eq!(events, expected);
    let retry = source.retry().expect("expect retry");
    assert!((Duration::from_millis(1000)..Duration::from_millis(5000)).contains(&retry));
    Ok(())
}

#[integration_test]
async fn sse() -> Result<(), Exception> {
    let (signal, mut signals) = mpsc::unbounded_channel();
    let router = Router::new()
        .state(Arc::new(AppState { signal }))
        .route(Method::GET, "/events", events)
        .route(Method::GET, "/forbidden", forbidden)
        .route(Method::GET, "/fail", fail)
        .route(Method::GET, "/ticks", ticks)
        .route(Method::GET, "/hold", hold)
        .route(Method::GET, "/silent", silent);
    let mut server = TestServer::start(router).await;

    for h2c in [false, true] {
        let client = &http_client(h2c);
        let mut request = HttpRequest::new(Method::GET, server.url("/events"))?;
        request.header(HeaderName::from_static("last-event-id"), "0")?;
        let response = client.execute(request).await?;
        assert_eq!(response.status, 200);
        assert_eq!(response.headers[header::CONTENT_TYPE], "text/event-stream");
        assert_eq!(response.headers[header::CACHE_CONTROL], "no-cache");
        assert!(response.headers.get(header::CONTENT_LENGTH).is_none());
        // the stream ends when the handler returns
        assert_eq!(response.text()?, "id: 1\ndata: a\ndata: b0\n\nevent: list\ndata: [\"x\"]\n\n");

        // the controller rejects before the stream starts, a normal error response
        let response = execute(client, Method::GET, server.url("/forbidden")).await?;
        assert_eq!(response.status, 403);
        let error: ErrorResponse = json::from_json(response.text()?)?;
        assert_eq!(error.code.as_deref(), Some(error_code::FORBIDDEN));

        // an error in the sse handler ends the stream normally, it is logged with the sse action
        let response = execute(client, Method::GET, server.url("/fail")).await?;
        assert_eq!(response.status, 200);
        assert_eq!(response.text()?, "data: before\n\n");

        // HEAD doesn't run the sse handler, it would signal "hold" before "ticks" below
        let response = execute(client, Method::HEAD, server.url("/hold")).await?;
        assert_eq!(response.status, 200);
        assert_eq!(response.headers[header::CONTENT_TYPE], "text/event-stream");
        assert!(response.body.is_empty());

        // client disconnect drops the handler
        let mut source = client.sse(HttpRequest::new(Method::GET, server.url("/ticks"))?).await?;
        let event = source.try_next().await?.expect("expect sse event");
        assert_eq!(event.data, "tick");
        drop(source);
        assert_eq!(next_signal(&mut signals).await, "ticks");
    }

    // the framework client parses back what the server writes
    let mut request = HttpRequest::new(Method::GET, server.url("/events"))?;
    request.header(HeaderName::from_static("last-event-id"), "0")?;
    let events: Vec<_> = http_client(false).sse(request).await?.try_collect().await?;
    let events: Vec<_> = events.into_iter().map(|event| (event.id, event.event_type, event.data)).collect();
    assert_eq!(
        events,
        [
            (Some("1".to_owned()), None, "a\nb0".to_owned()),
            (Some("1".to_owned()), Some("list".to_owned()), "[\"x\"]".to_owned())
        ]
    );

    let hold_sources = [
        http_client(false).sse(HttpRequest::new(Method::GET, server.url("/hold"))?).await?,
        http_client(true).sse(HttpRequest::new(Method::GET, server.url("/hold"))?).await?,
    ];
    // headers are sent before any event, sse() returns once it has a 200 text/event-stream response
    let silent_source = http_client(true).sse(HttpRequest::new(Method::GET, server.url("/silent"))?).await?;

    // open sse streams are closed on shutdown and don't block graceful drain, handlers are dropped
    server.shutdown().await;
    let mut finished =
        [next_signal(&mut signals).await, next_signal(&mut signals).await, next_signal(&mut signals).await];
    finished.sort_unstable();
    assert_eq!(finished, ["hold", "hold", "silent"]);

    for source in hold_sources {
        assert_retry(source, &["hello"]).await?;
    }
    assert_retry(silent_source, &[]).await?;
    Ok(())
}
