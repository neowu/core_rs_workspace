use framework::exception::Exception;
use framework_clickhouse::ClickHouse;

pub fn client() -> ClickHouse {
    ClickHouse::new("http://dev.internal:8123", "root", "root", None)
}

// wait_for_async_insert=0: insert() returns once the server buffered the batch
pub async fn flush(clickhouse: &ClickHouse) -> Result<(), Exception> {
    clickhouse.execute("SYSTEM FLUSH ASYNC INSERT QUEUE", &[]).await?;
    Ok(())
}
