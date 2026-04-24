use futures::TryFutureExt;
use tokio_postgres::Row;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;

use crate::db::Database;
use crate::db::PgError;
use crate::db::QueryParam;
use crate::exception;
use crate::exception::Exception;

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
pub trait Select {
    fn __get_sql() -> &'static str;
}

#[doc(hidden)] // disable auto complete, it's used by framework
pub trait Delete {
    fn __delete_sql() -> &'static str;
}

pub async fn insert<T: Insert>(database: &Database, entity: &T) -> Result<(), Exception> {
    async {
        let connection = database.pool.get_with_timeout().await?;
        let sql = T::__insert_sql();
        let params = entity.__insert_params();
        debug!("insert, sql={sql}, params={params:?}");

        let db_write_rows = connection
            .with_timeout(
                connection
                    .client
                    .execute(sql, &params)
                    .map_err(|err| exception!(message = "failed to insert", source = err)),
                database.query_timeout,
            )
            .await?;

        debug!(db_write_rows, "stats");
        Ok(())
    }
    .instrument(debug_span!("db"))
    .await
}

// return true if inserted
pub async fn insert_ignore<T: Insert>(database: &Database, entity: &T) -> Result<bool, Exception> {
    async {
        let connection = database.pool.get_with_timeout().await?;
        let sql = T::__insert_ignore_sql();
        let params = entity.__insert_params();
        debug!("insert_ignore, sql={sql}, params={params:?}");

        let db_write_rows = connection
            .with_timeout(
                connection
                    .client
                    .execute(sql, &params)
                    .map_err(|err| exception!(message = "failed to insert_ignore", source = err)),
                database.query_timeout,
            )
            .await?;

        debug!(db_write_rows, "stats");
        Ok(db_write_rows != 0)
    }
    .instrument(debug_span!("db"))
    .await
}

// return true if inserted
pub async fn upsert<T: Insert>(database: &Database, entity: &T) -> Result<bool, Exception> {
    async {
        let connection = database.pool.get_with_timeout().await?;
        let sql = T::__upsert_sql();
        let params = entity.__insert_params();
        debug!("upsert, sql={sql}, params={params:?}");

        let row = connection
            .with_timeout(
                connection
                    .client
                    .query_one(sql, &params)
                    .map_err(|err| exception!(message = "failed to upsert", source = err)),
                database.query_timeout,
            )
            .await?;

        let inserted: bool =
            row.try_get(0).map_err(|err| exception!(message = "failed to get result", source = err))?;
        debug!("inserted={inserted}");
        debug!(db_write_rows = 1, "stats"); // postgres upsert always affects row
        Ok(inserted)
    }
    .instrument(debug_span!("db"))
    .await
}

pub async fn insert_with_auto_increment_id<T: InsertWithAutoIncrementId>(
    database: &Database,
    entity: &T,
) -> Result<i64, Exception> {
    async {
        let connection = database.pool.get_with_timeout().await?;
        let sql = T::__insert_sql();
        let params = entity.__insert_params();
        debug!("insert, sql={sql}, params={params:?}");

        let row = connection
            .with_timeout(
                connection
                    .client
                    .query_one(sql, &params)
                    .map_err(|err| exception!(message = "failed to insert", source = err)),
                database.query_timeout,
            )
            .await?;

        let id: i64 = row.try_get(0).map_err(|err| exception!(message = "failed to get result", source = err))?;
        debug!(db_write_rows = 1, "stats");
        Ok(id)
    }
    .instrument(debug_span!("db"))
    .await
}

pub async fn get<T>(database: &Database, ids: &[&QueryParam]) -> Result<Option<T>, Exception>
where
    T: Select + TryFrom<Row, Error = PgError>,
{
    async {
        let mut connection = database.pool.get_with_timeout().await?;
        let sql = T::__get_sql();
        debug!("get, sql={sql}, params={ids:?}");
        let statement = connection.prepared_statement(sql).await?;

        let row = connection
            .with_timeout(
                connection
                    .client
                    .query_opt(&statement, ids)
                    .map_err(|err| exception!(message = "failed to select", source = err)),
                database.query_timeout,
            )
            .await?;

        let db_read_rows = if row.is_some() { 1 } else { 0 };
        debug!(db_read_rows, "stats");
        row.map(T::try_from).transpose().map_err(|err| exception!(message = "failed to map row", source = err))
    }
    .instrument(debug_span!("db"))
    .await
}

pub async fn delete<T: Delete>(database: &Database, ids: &[&QueryParam]) -> Result<bool, Exception> {
    async {
        let mut connection = database.pool.get_with_timeout().await?;
        let sql = T::__delete_sql();
        debug!("delete, sql={sql}, params={ids:?}");
        let statement = connection.prepared_statement(sql).await?;

        let db_write_rows = connection
            .with_timeout(
                connection
                    .client
                    .execute(&statement, ids)
                    .map_err(|err| exception!(message = "failed to delete", source = err)),
                database.query_timeout,
            )
            .await?;

        debug!(db_write_rows, "stats");
        Ok(db_write_rows != 0)
    }
    .instrument(debug_span!("db"))
    .await
}
