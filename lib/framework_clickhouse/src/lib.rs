use std::fmt::Debug;
use std::marker::PhantomData;

pub use clickhouse;
use clickhouse::_priv::RowKind;
use clickhouse::Client;
use clickhouse::Row;
use clickhouse::RowOwned;
use clickhouse::RowRead;
use clickhouse::query::Query;
use framework::console;
use framework::exception;
use framework::exception::Exception;
use framework::log;
use framework::span;
use framework::stats;
pub use framework_macro::Enum8;
use serde::Serialize;
pub mod types;

// clickhouse's Bind trait is sealed and not object-safe, so params can't be `&[&dyn Bind]`
// like framework_db's `&[&dyn ToSql]`; this wrapper folds each param into query.bind().
pub trait QueryParam: Debug + Sync {
    fn bind(&self, query: Query) -> Query;
}

impl<T: Serialize + Debug + Sync> QueryParam for T {
    fn bind(&self, query: Query) -> Query {
        query.bind(self)
    }
}

pub struct ClickHouse {
    client: Client,
}

impl ClickHouse {
    pub fn new(uri: &str, user: &str, password: &str, database: Option<&str>) -> Self {
        console!("create clickhouse client, uri={uri}, user={user}, db={database:?}");
        let client = Client::default().with_url(uri).with_user(user).with_password(password);
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
        query
            .execute()
            .await
            .map_err(|err| exception!("failed to execute statement", code = "CLICKHOUSE_ERROR", source = err))
    }

    pub async fn select_one<T>(&self, sql: &str, params: &[&dyn QueryParam]) -> Result<Option<T>, Exception>
    where
        T: RowOwned + RowRead,
    {
        let _span = span!("clickhouse");
        log!("select_one, sql={sql}, params={params:?}");
        let mut query = self.client.query(sql);
        for param in params {
            query = param.bind(query);
        }
        let row = query
            .fetch_optional()
            .await
            .map_err(|err| exception!("failed to select one", code = "CLICKHOUSE_ERROR", source = err))?;

        stats!(clickhouse_read_rows = if row.is_some() { 1 } else { 0 });

        Ok(row)
    }

    pub async fn select_all<T>(&self, sql: &str, params: &[&dyn QueryParam]) -> Result<Vec<T>, Exception>
    where
        T: RowOwned + RowRead,
    {
        let _span = span!("clickhouse");
        log!("select_all, sql={sql}, params={params:?}");
        let mut query = self.client.query(sql);
        for param in params {
            query = param.bind(query);
        }
        let rows = query
            .fetch_all()
            .await
            .map_err(|err| exception!("failed to select all", code = "CLICKHOUSE_ERROR", source = err))?;

        stats!(clickhouse_read_rows = rows.len());

        Ok(rows)
    }

    // rows may own their data or borrow it, e.g. a row pointing into the message it is written for,
    // so a batch costs no copy of the data it already has; either way the row type comes from the slice
    pub async fn insert<T>(&self, table: &str, rows: &[T]) -> Result<(), Exception>
    where
        T: Row + Serialize,
    {
        let _span = span!("clickhouse");
        // previously it used setting .with_setting("async_insert", "1").with_setting("wait_for_async_insert", "0");
        // but found silent data loss on clickhouse, no error on both side, no error in "system.asynchronous_insert_log"
        // so here to use without, message handler will wait until success
        let mut inserter = self.client.inserter::<FixedRow<T>>(table);
        for row in rows {
            inserter
                .write(row)
                .await
                .map_err(|err| exception!("failed to insert", code = "CLICKHOUSE_ERROR", source = err))?;
        }
        let quantities = inserter
            .end()
            .await
            .map_err(|err| exception!("failed to commit insert", code = "CLICKHOUSE_ERROR", source = err))?;
        stats!(clickhouse_write_rows = quantities.rows, clickhouse_write_bytes = quantities.bytes);
        Ok(())
    }
}

// clickhouse writes a `T::Value<'_>` - `T` re-bound to the slice's lifetime - and a projection
// can't be inverted to infer `T`, while bounding `T: Row<Value<'a> = T>` next to `RowWrite` trips
// rustc ("one type is more general than the other"). a row whose `Value<'_>` is `T` itself, of
// whatever lifetime, takes `&T` as is. forwards clickhouse's `#[doc(hidden)]` row metadata, which
// is only what `#[derive(Row)]` generates, but isn't covered by semver.
struct FixedRow<T>(PhantomData<T>);

impl<T: Row> Row for FixedRow<T> {
    const NAME: &'static str = T::NAME;
    const COLUMN_NAMES: &'static [&'static str] = T::COLUMN_NAMES;
    const COLUMN_COUNT: usize = T::COLUMN_COUNT;
    const KIND: RowKind = T::KIND;
    type Value<'a> = T;
}
