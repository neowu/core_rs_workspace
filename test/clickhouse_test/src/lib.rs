use std::time::Duration;

use framework::context;
use framework::exception::Exception;
use framework::log;
use framework_clickhouse::ClickHouse;
use framework_clickhouse::QueryParam;
use framework_clickhouse::clickhouse::RowOwned;
use framework_clickhouse::clickhouse::RowRead;

pub async fn run_test(name: &'static str, task: impl Future<Output = Result<(), Exception>>) {
    log::init("console", env!("CARGO_PKG_NAME"));
    log::action("test", None, async {
        context!(name = name);
        task.await
    })
    .await
    .unwrap();
}

pub fn client(database: Option<&str>) -> ClickHouse {
    ClickHouse::new("http://dev.internal:8123", "root", "root", database)
}

// wait_for_async_insert=0: insert() returns once the server buffered the batch,
// so tests must poll until the batch is flushed to the table and the row becomes visible
pub async fn select_one_with_retry<T>(
    clickhouse: &ClickHouse,
    sql: &str,
    params: &[&dyn QueryParam],
) -> Result<T, Exception>
where
    T: RowOwned + RowRead,
{
    let mut attempts = 0;
    loop {
        let result = clickhouse.select_one(sql, params).await;
        attempts += 1;
        if result.is_ok() || attempts >= 20 {
            return result;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}
