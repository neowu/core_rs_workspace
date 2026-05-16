use std::sync::Arc;
use std::time::Duration;

use framework::exception;
use framework::exception::Exception;
use framework::pool::ResourcePool;
use tokio_postgres::Config;
use tracing::Instrument as _;
use tracing::debug;
use tracing::debug_span;

use crate::FromRow;
use crate::QueryParam;
use crate::connection::ConnectionManager;

pub struct Database {
    pub(super) pool: Arc<ResourcePool<ConnectionManager>>,
    pub(super) query_timeout: Duration,
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
        row.map(T::try_from).transpose().map_err(|err| exception!("failed to map row", source = err))
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
            .map_err(|err| exception!("failed to map row", source = err))
    }
    .instrument(debug_span!("db"))
    .await
}
