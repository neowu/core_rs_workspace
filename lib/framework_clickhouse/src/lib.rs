// #[serde(with = "clickhouse::serde::...")] field paths resolve `clickhouse::` in the caller's
// module scope, and the framework_macro::Row derive expands to `framework_clickhouse::clickhouse::`
// paths, so apps use this re-export instead of depending on the clickhouse crate directly.
use std::fmt::Debug;

pub use clickhouse;
use clickhouse::Client;
use clickhouse::RowOwned;
use clickhouse::RowRead;
use clickhouse::RowWrite;
use clickhouse::query::Query;
use clickhouse::sql;
use framework::exception;
use framework::exception::Exception;
use framework::log;
use framework::span;
use framework::stats;
use serde::Serialize;
pub use serde_repr::Serialize_repr;

// local newtype so the impl doesn't conflict with the Serialize blanket impl
#[derive(Debug)]
pub struct Identifier<'a>(pub &'a str);

// clickhouse's Bind trait is sealed and not object-safe, so params can't be `&[&dyn Bind]`
// like framework_db's `&[&dyn ToSql]`; this wrapper folds each param into query.bind().
pub trait QueryParam: Debug {
    fn bind(&self, query: Query) -> Query;
}

impl QueryParam for Identifier<'_> {
    fn bind(&self, query: Query) -> Query {
        query.bind(sql::Identifier(self.0))
    }
}

impl<T: Serialize + Debug> QueryParam for T {
    fn bind(&self, query: Query) -> Query {
        query.bind(self)
    }
}

pub struct ClickHouse {
    client: Client,
}

impl ClickHouse {
    pub fn new(uri: &str, user: &str, password: &str, database: Option<&str>) -> Self {
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

    // each `?` in sql is replaced client-side by the corresponding param, in order; use `??` for a literal `?`
    pub async fn execute(&self, sql: &str, params: &[&dyn QueryParam]) -> Result<(), Exception> {
        let _span = span!("clickhouse");
        log!("execute, sql={sql}, params={params:?}");
        let mut query = self.client.query(sql);
        for param in params {
            query = param.bind(query);
        }
        query.execute().await.map_err(|err| exception!("failed to execute statement", source = err))
    }

    pub async fn select_one<T>(&self, sql: &str, params: &[&dyn QueryParam]) -> Result<T, Exception>
    where
        T: RowOwned + RowRead,
    {
        let _span = span!("clickhouse");
        log!("select_one, sql={sql}, params={params:?}");
        let mut query = self.client.query(sql);
        for param in params {
            query = param.bind(query);
        }
        query.fetch_one().await.map_err(|err| exception!("failed to execute statement", source = err))
    }

    // async_insert is enabled on the client, so end() hands the batch to the server and returns
    // without waiting for the on-disk flush (wait_for_async_insert=0); the server batches across requests.
    pub async fn insert<T>(&self, table: &str, rows: &[T]) -> Result<(), Exception>
    where
        T: RowOwned + RowWrite,
    {
        let _span = span!("clickhouse");
        // Inserter accumulates the serialized byte count and row count, returned as Quantities by end().
        // fully qualified because the hidden clickhouse::Row trait also declares a NAME const
        let mut inserter = self.client.inserter::<T>(table);
        for row in rows {
            inserter.write(row).await.map_err(|err| exception!("failed to insert", source = err))?;
        }
        let quantities = inserter.end().await.map_err(|err| exception!("failed to commit insert", source = err))?;
        stats!(clickhouse_write_rows = quantities.rows, clickhouse_write_bytes = quantities.bytes);
        Ok(())
    }
}
