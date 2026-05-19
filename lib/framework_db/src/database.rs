use framework::exception;
use framework::exception::Exception;
use framework::stats;
use tracing::Instrument as _;
use tracing::debug;
use tracing::debug_span;

use crate::Database;
use crate::FromRow;
use crate::QueryParam;

pub async fn execute(database: &Database, statement: &str, params: &[&QueryParam]) -> Result<u64, Exception> {
    async {
        let conn = database.pool.get_with_timeout().await?;
        debug!("execute, sql={statement}, params={params:?}");
        let db_write_rows = conn.with_timeout(conn.client.execute(statement, params), database.query_timeout).await?;
        stats!(db_write_rows);
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
        stats!(db_read_rows = if row.is_some() { 1 } else { 0 });
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
        stats!(db_read_rows = rows.len());
        rows.into_iter()
            .map(T::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| exception!("failed to map row", source = err))
    }
    .instrument(debug_span!("db"))
    .await
}
