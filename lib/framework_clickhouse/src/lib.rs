use clickhouse::Client;
use clickhouse::RowOwned;
use clickhouse::RowWrite;
// #[serde(with = "clickhouse::serde::...")] field paths resolve `clickhouse::` in the caller's
// module scope, and the framework_macro::Row derive expands to `framework_clickhouse::clickhouse::`
// paths, so apps use this re-export instead of depending on the clickhouse crate directly.
pub use clickhouse;
pub use serde_repr::Serialize_repr;
use framework::exception;
use framework::exception::Exception;
use framework::log;
use framework::span;
use framework::stats;

// implemented by #[derive(framework_macro::Row)] via #[table(name = "...")]
pub trait Table {
    const NAME: &'static str;
}

pub struct ClickHouse {
    client: Client,
}

impl ClickHouse {
    pub fn new(uri: String, user: String, password: &str, database: Option<&str>) -> Self {
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

    pub async fn execute(&self, sql: &str) -> Result<(), Exception> {
        let _span = span!("clickhouse");
        log!("sql={sql}");
        self.client.query(sql).execute().await.map_err(|err| exception!("failed to execute statement", source = err))
    }

    // async_insert is enabled on the client, so end() hands the batch to the server and returns
    // without waiting for the on-disk flush (wait_for_async_insert=0); the server batches across requests.
    pub async fn insert<T>(&self, rows: &[T]) -> Result<(), Exception>
    where
        T: RowOwned + RowWrite + Table,
    {
        let _span = span!("clickhouse");
        // Inserter accumulates the serialized byte count and row count, returned as Quantities by end().
        // fully qualified because the hidden clickhouse::Row trait also declares a NAME const
        let mut inserter = self.client.inserter::<T>(<T as Table>::NAME);
        for row in rows {
            inserter.write(row).await.map_err(|err| exception!("failed to insert", source = err))?;
        }
        let quantities = inserter.end().await.map_err(|err| exception!("failed to commit insert", source = err))?;
        stats!(clickhouse_write_rows = quantities.rows, clickhouse_write_bytes = quantities.bytes);
        Ok(())
    }
}
