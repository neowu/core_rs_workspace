use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use framework::exception::Exception;
use framework::http::Method;
use framework::log;
use framework::web::diagnostic;
use framework::web::request::Request;
use framework::web::response::Response;
use framework::web::router::Router;
use framework_macro::integration_test;
use http_test::TestServer;
use http_test::execute;
use http_test::http_client;
use tokio::sync::Notify;
use tokio::time::sleep;

#[derive(Default)]
struct AppState {
    started: Notify,
    release: Notify,
}

async fn slow(state: Arc<AppState>, _request: Request) -> Result<Response, Exception> {
    log!(r#"waiting, body={{"password":"secret"}}"#);
    state.started.notify_one();
    state.release.notified().await;
    Ok(Response::text("done"))
}

#[integration_test]
async fn in_flight_actions() -> Result<(), Exception> {
    let state = Arc::new(AppState::default());
    let mut server = TestServer::start(Router::state(Arc::clone(&state)).route(Method::GET, "/slow", slow)).await;
    // sys routes are served by their own server, as an app wires them
    let mut sys_server = TestServer::start(diagnostic::routes()).await;
    let client = http_client(false);

    let slow_client = Arc::clone(&client);
    let url = server.url("/slow");
    let slow_request = tokio::spawn(async move { execute(&slow_client, Method::GET, url).await });
    state.started.notified().await;

    // the test action and the request waiting in the handler, with what it has logged so far;
    // the handler is still being polled right after it signals, wait until it is suspended
    let deadline = Instant::now() + Duration::from_secs(2);
    let dump = loop {
        let response = execute(&client, Method::GET, sys_server.url("/sys/action")).await?;
        let dump = response.text()?.to_owned();
        if dump.contains("[context] path=/slow") {
            break dump;
        }
        assert!(Instant::now() < deadline, "slow request must be suspended, dump={dump}");
        sleep(Duration::from_millis(10)).await;
    };
    assert!(dump.starts_with("in_flight_actions=2\n"), "dump={dump}");
    let entry = dump
        .split("\n\n")
        .find(|entry| entry.contains("[context] path=/slow"))
        .unwrap_or_else(|| panic!("slow request must be in flight, dump={dump}"));
    assert!(entry.starts_with("# [in-flight] id="), "entry={entry}");
    assert!(entry.contains("kind=http, state=suspended, elapsed="), "entry={entry}");
    assert!(entry.contains(r#"waiting, body={"password":"**masked**"}"#), "entry={entry}");
    // the dump request is not listed
    assert!(!dump.contains("path=/sys/action"), "dump={dump}");

    state.release.notify_one();
    let response = slow_request.await.expect("slow request must not panic")?;
    assert_eq!(response.text()?, "done");

    let response = execute(&client, Method::GET, sys_server.url("/sys/action")).await?;
    let dump = response.text()?;
    // only the test action is left, its trace has the previous dump, so match the count
    assert!(dump.starts_with("in_flight_actions=1\n"), "dump={dump}");

    // every thread with its stack, the thread serving the request is polling its http action
    let response = execute(&client, Method::GET, sys_server.url("/sys/thread")).await?;
    let dump = response.text()?;
    assert!(dump.starts_with("threads="), "dump={dump}");
    let serving = dump
        .split("\n\n")
        .find(|thread| thread.contains("framework::web::diagnostic::thread"))
        .unwrap_or_else(|| panic!("serving thread must be in dump, dump={dump}"));
    assert!(serving.starts_with("# [thread] id="), "thread={serving}");
    assert!(serving.contains(", action="), "thread={serving}");

    // not on the app server
    for path in ["/sys/action", "/sys/thread"] {
        let response = execute(&client, Method::GET, server.url(path)).await?;
        assert_eq!(response.status, 404);
    }

    sys_server.shutdown().await;
    server.shutdown().await;
    Ok(())
}
