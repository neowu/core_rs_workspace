use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use framework::context;
use framework::exception;
use framework::exception::Exception;
use framework::exception::Severity;
use framework::log;
use framework::shell;
use framework::spawn_action;
use framework::stats;
use framework::task;
use tokio::task::yield_now;
use tracing::Instrument as _;
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
    log::init();
    log::init_appender("console", env!("CARGO_BIN_NAME")).unwrap();

    test_action().await;

    task::shutdown(Duration::from_secs(15)).await;
}

async fn test_action() {
    let _ = log::start_action("some-action", None, async {
        let x = Arc::new(Mutex::new(1));
        let y = x.clone();

        context!(key = "value1", key2 = "value2");

        stats!(write_bytes = 23);

        spawn_action!("some-task", async move {
            context!(location = concat!(file!(), ":", line!()));
            *y.lock().unwrap() = 2;
            warn!(error_code = "TEST", "trigger");
            shell::run("echo 'Hello, World!'").await?;
            Ok(())
        });

        context!(key4 = "value4");
        warn!("after task, {}", x.lock().unwrap());
        handle_request(false).await?;
        Ok(())
    })
    .await;
}

#[instrument]
async fn handle_request(success: bool) -> Result<(), Exception> {
    let span = info_span!("http", test_value = field::Empty, elapsed = field::Empty);
    async {
        info!(request_id = 123, "Processing request,");

        Span::current().record("test_value", "yes");

        info!("after span record");
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
        warn!(error_code = "SOMETHING", status = "failure", "Something went wrong");
        error!(error_code = "DB", reason = "database_error", "Could not connect to database");
        Err(exception!(
            format!("key length must be 16 characters, got {:?}", "key"),
            severity = Severity::Warn,
            code = "E001"
        ))
    }
}

async fn other_method() {
    info!("other_method");
}
