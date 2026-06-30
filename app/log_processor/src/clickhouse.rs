use std::collections::HashMap;

use clickhouse::Client;
use clickhouse::Row;
use clickhouse::RowOwned;
use clickhouse::RowWrite;
use framework::exception;
use framework::exception::Exception;
use framework::log;
use framework::span;
use framework::stats;
use serde::Serialize;
use serde_repr::Serialize_repr;
use time::OffsetDateTime;

pub(crate) struct ClickHouse {
    client: Client,
}

impl ClickHouse {
    pub(crate) fn new(uri: String, user: String, password: &str, database: Option<&str>) -> Self {
        // async_insert lets the server batch writes; wait_for_async_insert=0 returns once buffered, not flushed.
        // inserts added later inherit these settings from the shared client.
        let client = Client::default()
            .with_url(uri)
            .with_user(user)
            .with_password(password)
            .with_setting("async_insert", "1")
            .with_setting("wait_for_async_insert", "0");
        let client = if let Some(database) = database { client.with_database(database) } else { client };

        Self { client }
    }

    pub(crate) async fn execute(&self, sql: &str) -> Result<(), Exception> {
        let _span = span!("clickhouse");
        log!("sql={sql}");
        self.client.query(sql).execute().await.map_err(|err| exception!("failed to execute statement", source = err))
    }

    // async_insert is enabled on the client, so end() hands the batch to the server and returns
    // without waiting for the on-disk flush (wait_for_async_insert=0); the server batches across requests.
    pub(crate) async fn insert<T>(&self, table: &str, rows: &[T]) -> Result<(), Exception>
    where
        T: RowOwned + RowWrite,
    {
        let _span = span!("clickhouse");
        // `table` is bare (e.g. "action"); the `log` database comes from the client.
        // Inserter accumulates the serialized byte count and row count, returned as Quantities by end().
        let mut inserter = self.client.inserter::<T>(table);
        for row in rows {
            inserter.write(row).await.map_err(|err| exception!("failed to insert", source = err))?;
        }
        let quantities = inserter.end().await.map_err(|err| exception!("failed to commit insert", source = err))?;
        stats!(clickhouse_write_rows = quantities.rows, clickhouse_write_bytes = quantities.bytes);
        Ok(())
    }
}

// row written to log.action; field names and order match the table columns for RowBinary insert.
#[derive(Row, Serialize)]
pub(crate) struct ActionRow {
    // DateTime64(3, 'UTC') is encoded as i64 milliseconds; the time feature provides this serde helper.
    #[serde(with = "clickhouse::serde::time::datetime64::millis")]
    pub time: OffsetDateTime,
    pub id: String,
    pub app: String,
    pub host: String,
    pub result: ActionResult,
    pub action: String,
    pub ref_id: Option<String>,
    pub ref_ids: Vec<String>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub context: HashMap<String, String>,
    pub multi_context: HashMap<String, Vec<String>>,
    pub stats: HashMap<String, i64>,
}

// Enum8('OK' = 1, 'WARN' = 2, 'ERROR' = 3); serialized as its i8 discriminant.
#[derive(Serialize_repr)]
#[repr(i8)]
pub(crate) enum ActionResult {
    Ok = 1,
    Warn = 2,
    Error = 3,
}
