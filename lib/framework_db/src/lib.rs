use std::str::FromStr as _;
use std::sync::Arc;
use std::time::Duration;

pub use field::Cond;
pub use field::Field;
pub use field::Update;
use framework::console;
use framework::exception;
use framework::exception::Exception;
use framework::log::metrics::Metrics;
use framework::pool::ResourcePool;
pub use tokio_postgres::Config;
pub use tokio_postgres::Error as PgError;
pub use tokio_postgres::Row;
use tokio_postgres::types::FromSqlOwned;
pub use tokio_postgres::types::Json;
pub use tokio_postgres::types::ToSql;

use crate::connection::ConnectionManager;

mod connection;
pub mod database;
mod field;
pub mod repository;

pub type QueryParam = dyn ToSql + Sync;

pub trait FromRow: Sized {
    fn try_from(row: Row) -> Result<Self, PgError>;
}

impl<T> FromRow for T
where
    T: FromSqlOwned,
{
    fn try_from(row: Row) -> Result<Self, PgError> {
        row.try_get(0)
    }
}

#[doc(hidden)] // disable auto complete, it's used by framework
pub trait InsertWithAutoIncrementId {
    fn __insert_sql() -> &'static str;
    fn __insert_params(&self) -> Vec<&QueryParam>;
}

#[doc(hidden)] // disable auto complete, it's used by framework
pub trait Insert {
    fn __insert_sql() -> &'static str;
    fn __insert_ignore_sql() -> &'static str;
    fn __upsert_sql() -> &'static str;
    fn __insert_params(&self) -> Vec<&QueryParam>;
}

#[doc(hidden)] // disable auto complete, it's used by framework
pub trait Entity {
    type Id;
    fn __id_conditions(ids: &Self::Id) -> Vec<Cond<'_, Self>>
    where
        Self: Sized;
    fn __table_name() -> &'static str;
    fn __select_sql() -> &'static str;
}

pub struct Database {
    pool: Arc<ResourcePool<ConnectionManager>>,
    query_timeout: Duration,
}

pub struct DbConfig {
    pub uri: String,
    pub user: String,
    pub password: String,
    pub client: &'static str, // pass as env!("CARGO_BIN_NAME") or env!("CARGO_PKG_NAME")
}

impl Database {
    pub fn new(config: DbConfig) -> Result<Self, Exception> {
        console!("create database client, uri={}, user={}", config.uri, config.user);
        let mut postgres_config =
            Config::from_str(&config.uri).map_err(|err| exception!("failed to parse postgres uri", source = err))?;
        postgres_config.user(config.user);
        postgres_config.password(config.password);
        postgres_config.connect_timeout(Duration::from_secs(5));
        postgres_config.application_name(config.client);

        let pool = Arc::new(ResourcePool::new(
            ConnectionManager { config: postgres_config },
            50,
            Duration::from_secs(30),
            Duration::from_hours(1),
            Duration::from_secs(5),
        ));

        Ok(Database { pool, query_timeout: Duration::from_secs(5) })
    }

    pub fn db_metrics(&self) -> impl Fn(&mut Metrics) + Send + 'static {
        let pool = Arc::clone(&self.pool);
        move |metrics| metrics.stats.push(("active_db_conns", pool.active_count() as u64))
    }
}
