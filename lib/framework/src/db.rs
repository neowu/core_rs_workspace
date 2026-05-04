use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

pub use query::Cond;
pub use query::Update;
use tokio::task;
use tokio::time::timeout;
use tokio_postgres::CancelToken;
pub use tokio_postgres::Client;
pub use tokio_postgres::Config;
pub use tokio_postgres::Error as PgError;
use tokio_postgres::NoTls;
pub use tokio_postgres::Row;
use tokio_postgres::Statement;
use tokio_postgres::types::FromSqlOwned;
pub use tokio_postgres::types::ToSql;
use tracing::Instrument as _;
use tracing::debug;
use tracing::debug_span;
use tracing::error;

use crate::exception;
use crate::exception::Exception;
use crate::pool::ResourceManager;
use crate::pool::ResourcePool;

pub mod query;
pub mod repository;

struct Connection {
    client: Client,
    cancel_token: CancelToken,
    statement_cache: HashMap<String, Statement>,
}

impl Connection {
    async fn prepared_statement(&mut self, sql: &str) -> Result<Statement, Exception> {
        if let Some(statement) = self.statement_cache.get(sql) {
            Ok(statement.clone())
        } else {
            let statement = self
                .client
                .prepare(sql)
                .await
                .map_err(|err| exception!(message = "failed to prepare statement", source = err))?;
            self.statement_cache.insert(sql.to_owned(), statement.clone());
            Ok(statement)
        }
    }

    async fn with_timeout<T>(
        &self,
        operation: impl Future<Output = Result<T, PgError>>,
        query_timeout: Duration,
    ) -> Result<T, Exception> {
        let result = timeout(query_timeout, operation).await;
        match result {
            Ok(result) => result.map_err(|err| exception!(message = "failed to call db", source = err)),
            Err(_elapsed) => {
                debug!("cancel query");
                let cancel_result = self.cancel_token.cancel_query(NoTls).await;
                match cancel_result {
                    Ok(()) => Err(exception!(message = "query timed out")),
                    Err(err) => Err(exception!(message = "query timed out, failed to cancel", source = err)),
                }
            }
        }
    }
}

pub type QueryParam = dyn ToSql + Sync;

impl ResourceManager for ConnectionManager {
    type Target = Connection;

    async fn create(&self) -> Result<Self::Target, Exception> {
        let (client, connection) = self.config.connect(NoTls).await?;

        // use native tokio spawn, not wire current span
        task::spawn(async {
            if let Err(e) = connection.await {
                error!("connection error: {e}");
            }
        });

        let cancel_token = client.cancel_token();

        Ok(Connection { client, cancel_token, statement_cache: HashMap::new() })
    }

    async fn is_valid(item: &Self::Target) -> bool {
        item.client.check_connection().await.is_ok()
    }

    fn is_closed(item: &Self::Target) -> bool {
        item.client.is_closed()
    }
}

struct ConnectionManager {
    config: Config,
}

pub struct Database {
    pool: Arc<ResourcePool<ConnectionManager>>,
    query_timeout: Duration,
}

impl Database {
    pub fn new(config: Config) -> Self {
        let pool = Arc::new(ResourcePool::new(
            ConnectionManager { config },
            50,
            Duration::from_secs(30),
            Duration::from_hours(1),
            Duration::from_secs(5),
        ));
        Database { pool, query_timeout: Duration::from_secs(5) }
    }
}

pub async fn execute(database: &Database, statement: &str, params: &[&QueryParam]) -> Result<u64, Exception> {
    async {
        let conn = database.pool.get_with_timeout().await?;
        debug!("execute, sql={statement}, params={params:?}");
        let db_write_rows = conn.with_timeout(conn.client.execute(statement, params), database.query_timeout).await?;
        debug!(db_write_rows, "stats");
        Ok(db_write_rows)
    }
    .instrument(debug_span!("db"))
    .await
}

pub async fn select_one<T>(database: &Database, statement: &str, params: &[&QueryParam]) -> Result<Option<T>, Exception>
where
    T: FromRow,
{
    async {
        let conn = database.pool.get_with_timeout().await?;
        debug!("select_one, sql={statement}, params={params:?}");
        let row = conn.with_timeout(conn.client.query_opt(statement, params), database.query_timeout).await?;
        debug!(db_read_rows = if row.is_some() { 1 } else { 0 }, "stats");
        row.map(T::try_from).transpose().map_err(|err| exception!(message = "failed to map row", source = err))
    }
    .instrument(debug_span!("db"))
    .await
}

pub async fn select<T>(database: &Database, statement: &str, params: &[&QueryParam]) -> Result<Vec<T>, Exception>
where
    T: FromRow,
{
    async {
        let conn = database.pool.get_with_timeout().await?;
        debug!("select, sql={statement}, params={params:?}");
        let rows = conn.with_timeout(conn.client.query(statement, params), database.query_timeout).await?;
        debug!(db_read_rows = rows.len(), "stats");
        rows.into_iter()
            .map(T::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| exception!(message = "failed to map row", source = err))
    }
    .instrument(debug_span!("db"))
    .await
}

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
