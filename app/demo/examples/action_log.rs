use std::sync::Arc;
use std::sync::Mutex;

use framework::exception;
use framework::exception::CoreRsResult;
use framework::exception::Severity;
use framework::log;
use framework::log::ConsoleAppender;
use framework::shell;
use framework::task;
use tokio::task::yield_now;
use tracing::Instrument;
use tracing::Span;
use tracing::debug;
use tracing::debug_span;
use tracing::error;
use tracing::field;
use tracing::info;
use tracing::info_span;
use tracing::instrument;
use tracing::warn;

#[tokio::main]
async fn main() {
    log::init_with_action(ConsoleAppender);

    test_action().await;

    task::shutdown().await;
}

async fn test_action() {
    log::start_action("some-action", None, async {
        let x = Arc::new(Mutex::new(1));
        let y = x.clone();

        debug!(key = "value1", key2 = "value2", "context");

        task::spawn_action("some-task", async move {
            *y.lock().unwrap() = 2;
            warn!(error_code = "TEST", "y = {y:?}");
            shell::run("echo1 'Hello, World!'").await?;
            Ok(())
        });

        debug!(key3 = "value3", "context");

        warn!("after task, {}", x.lock().unwrap());
        handle_request(false).await?;
        Ok(())
    })
    .await;
}

#[instrument]
async fn handle_request(success: bool) -> CoreRsResult<()> {
    let span = info_span!("http", test_value = field::Empty, elapsed = field::Empty);
    async {
        info!(request_id = 123, "Processing request,");

        Span::current().record("test_value", "yes");
    }
    .instrument(span)
    .await;

    async {
        info!("inside async block");
    }
    .await;

    let db_span = debug_span!("db", elapsed = field::Empty);
    async {
        debug!(sql = "select 1", "run db query,");
    }
    .instrument(db_span)
    .await;

    yield_now().await;

    other_method().await;

    if success {
        info!(status = "success", "Request completed successfully,");
        Ok(())
    } else {
        warn!(error_code = "SOMETHING", status = "failure", "Something went wrong,");
        error!(
            error_code = "DB",
            reason = "database_error",
            "Could not connect to database,"
        );
        Err(exception!(
            severity = Severity::Warn,
            code = "E001",
            message = format!("key length must be 16 characters, got {:?}", "key")
        ))
    }
}

async fn other_method() {
    info!("other_method");
}
