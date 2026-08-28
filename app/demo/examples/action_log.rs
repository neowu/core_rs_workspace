use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use framework::context;
use framework::exception;
use framework::exception::Exception;
use framework::log;
use framework::log::ConsoleAppender;
use framework::log::Severity;
use framework::shell;
use framework::span;
use framework::spawn_action;
use framework::stats;
use framework::system::System;
use framework::warn;
use tokio::task::yield_now;

#[tokio::main]
async fn main() -> Result<(), Exception> {
    let mut system = System::init(env!("CARGO_BIN_NAME"));
    system.start_action_logger(ConsoleAppender);

    test_action();

    system.wait().await;
    system.shutdown(Duration::from_secs(15)).await
}

fn test_action() {
    let x = Arc::new(Mutex::new(1));
    let y = Arc::clone(&x);
    spawn_action!("some-action", async move {
        context!(key = "value1", key2 = vec!["value2", "value_22", "value23"]);

        stats!(write_bytes = 23);

        {
            let long_span = span!("long");
            for i in 0..1000 {
                long_span.clear();
                log!("message, i={i}");
            }
        }

        context!(key4 = "value4");
        log!("after task, {}", x.lock().unwrap());
        handle_request().await?;
        Ok(())
    });

    spawn_action!("some-task", async move {
        context!(location = concat!(file!(), ":", line!()));
        *y.lock().unwrap() = 2;
        warn!(error_code = "TEST", "trigger");
        shell::run("echo 'Hello, World!'").await?;
        Ok(())
    });
}

async fn handle_request() -> Result<(), Exception> {
    let _span = span!("http");
    log!("Processing request, request_id = 123");

    async {
        log!("inside async block");
    }
    .await;

    {
        let _db_span = span!("db");
        log!("run db query, sql=select 1");
    }
    yield_now().await;

    other_method();

    Err(exception!(
        format!("key length must be 16 characters, got {:?}", "key"),
        severity = Severity::Warn,
        code = "E001"
    ))
}

fn other_method() {
    log!("other_method");
}
