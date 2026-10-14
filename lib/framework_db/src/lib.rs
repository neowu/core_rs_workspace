use std::ops::DerefMut;
use std::str::FromStr as _;
use std::sync::Arc;
use std::time::Duration;

pub use field::Cond;
pub use field::Field;
pub use field::Update;
use framework::console;
use framework::exception;
use framework::exception::Exception;
use framework::metrics::Metrics;
use framework::pool::ResourceGuard;
use framework::pool::ResourcePool;
pub use tokio_postgres::Config;
pub use tokio_postgres::Error as PgError;
pub use tokio_postgres::Row;
use tokio_postgres::types::FromSqlOwned;
pub use tokio_postgres::types::Json;
pub use tokio_postgres::types::ToSql;
pub use transaction::Transaction;

#[doc(hidden)]
pub use crate::connection::Connection;
use crate::connection::ConnectionManager;

mod connection;
pub mod database;
mod field;
pub mod repository;
mod transaction;
pub mod types;

// a borrowed query param, shaped exactly as tokio_postgres takes them: `&[&(dyn ToSql + Sync)]`.
// `Send` cannot be added here, a `Vec<&(dyn ToSql + Sync + Send)>` does not coerce to that slice.
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
    fn __table_name() -> &'static str;
    fn __select_sql() -> &'static str;
}

// run statements on `&Database` (one pooled connection per call) or `&mut Transaction` (pinned connection)
pub trait Executor {
    #[doc(hidden)]
    fn __conn(
        &mut self,
    ) -> impl Future<Output = Result<impl DerefMut<Target = Connection> + Send + '_, Exception>> + Send;
}

impl Executor for &Database {
    async fn __conn(&mut self) -> Result<impl DerefMut<Target = Connection> + Send + '_, Exception> {
        self.checkout().await
    }
}

pub struct Database {
    pool: Arc<ResourcePool<ConnectionManager>>,
    uri: &'static str,
}

pub struct DbConfig {
    pub uri: String,
    pub user: String,
    pub password: String,
    pub client: &'static str, // pass as env!("CARGO_BIN_NAME") or env!("CARGO_PKG_NAME")
    pub query_timeout: Duration,
    pub pool_max_size: usize,
}

impl Default for DbConfig {
    fn default() -> Self {
        DbConfig {
            uri: "postgres://localhost:5432/postgres".to_owned(),
            user: "postgres".to_owned(),
            password: String::new(),
            client: "",
            query_timeout: Duration::from_secs(5),
            pool_max_size: 50,
        }
    }
}

impl Database {
    pub fn new(config: DbConfig) -> Self {
        console!("create database client, uri={}, user={}", config.uri, config.user);
        assert!(config.pool_max_size > 0, "pool_max_size must be positive");
        let mut postgres_config =
            Config::from_str(&config.uri).unwrap_or_else(|err| panic!("failed to parse postgres uri, error={err}"));
        postgres_config.user(config.user);
        postgres_config.password(config.password);
        postgres_config.connect_timeout(Duration::from_secs(5));
        if !config.client.is_empty() {
            postgres_config.application_name(config.client);
        }
        // uri holds no credentials by convention, leaked once per pool to be referenced in errors
        let uri: &'static str = config.uri.leak();
        let pool = Arc::new(ResourcePool::new(
            ConnectionManager { config: postgres_config, query_timeout: config.query_timeout, uri },
            config.pool_max_size,
            Duration::from_secs(30),
            Duration::from_hours(1),
            Duration::from_secs(5),
        ));

        Database { pool, uri }
    }

    pub async fn begin(&self) -> Result<Transaction<'_>, Exception> {
        Transaction::begin(self.checkout().await?).await
    }

    async fn checkout(&self) -> Result<ResourceGuard<'_, ConnectionManager>, Exception> {
        self.pool.checkout().await.map_err(|err| {
            exception!(format!("failed to get db connection, uri={}", self.uri), code = "DB_ERROR", source = err)
        })
    }

    pub fn metrics(&self) -> impl Fn(&mut Metrics) + Send + 'static {
        let pool = Arc::clone(&self.pool);
        move |metrics| metrics.add_stat("active_db_conns", pool.active_count() as u64)
    }
}
