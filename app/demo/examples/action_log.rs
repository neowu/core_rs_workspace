use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use demo::AppConfig;
use framework::context;
use framework::exception;
use framework::exception::Exception;
use framework::exception::Severity;
use framework::load_config;
use framework::log;
use framework::shell;
use framework::span;
use framework::spawn_action;
use framework::stats;
use framework::task;
use framework::warn;
use tokio::task::yield_now;

#[tokio::main]
async fn main() {
    let config: AppConfig = load_config!("assets/conf.json");
    log::init(&config.log_appender, env!("CARGO_PKG_NAME"));

    test_action().await;

    task::shutdown(Duration::from_secs(15)).await;
}

async fn test_action() {
    let _ = log::start_action("some-action", None, async {
        let x = Arc::new(Mutex::new(1));
        let y = x.clone();

        context!(key = "value1", key2 = "value2");

        stats!(write_bytes = 23);

        {
            let long_span = span!("long");
            for i in 0..1000 {
                long_span.clear();
                log!("message, i={i}");
            }
        }

        spawn_action!("some-task", async move {
            context!(location = concat!(file!(), ":", line!()));
            *y.lock().unwrap() = 2;
            warn!(error_code = "TEST", "trigger");
            shell::run("echo 'Hello, World!'").await?;
            Ok(())
        });

        context!(key4 = "value4");
        log!("after task, {}", x.lock().unwrap());
        handle_request().await?;
        Ok(())
    })
    .await;
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

    other_method().await;

    Err(exception!(
        format!("key length must be 16 characters, got {:?}", "key"),
        severity = Severity::Warn,
        code = "E001"
    ))
}

async fn other_method() {
    log!("other_method");
}
