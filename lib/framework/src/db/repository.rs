use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;

use crate::db::Database;
use crate::db::FromRow;
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
pub trait Entity {
    type Id;
    fn __id_params(ids: &Self::Id) -> Vec<&QueryParam>;
    fn __get_sql() -> &'static str;
    fn __select_sql() -> &'static str;
    fn __delete_sql() -> &'static str;
}

pub async fn insert<T: Insert>(database: &Database, entity: &T) -> Result<(), Exception> {
    async {
        let conn = database.pool.get_with_timeout().await?;
        let sql = T::__insert_sql();
        let params = entity.__insert_params();
        debug!("insert, sql={sql}, params={params:?}");
        let db_write_rows = conn.with_timeout(conn.client.execute(sql, &params), database.query_timeout).await?;
        debug!(db_write_rows, "stats");
        Ok(())
    }
    .instrument(debug_span!("db"))
    .await
}

// return true if inserted
pub async fn insert_ignore<T: Insert>(database: &Database, entity: &T) -> Result<bool, Exception> {
    async {
        let conn = database.pool.get_with_timeout().await?;
        let sql = T::__insert_ignore_sql();
        let params = entity.__insert_params();
        debug!("insert_ignore, sql={sql}, params={params:?}");
        let db_write_rows = conn.with_timeout(conn.client.execute(sql, &params), database.query_timeout).await?;
        debug!(db_write_rows, "stats");
        Ok(db_write_rows != 0)
    }
    .instrument(debug_span!("db"))
    .await
}

// return true if inserted
pub async fn upsert<T: Insert>(database: &Database, entity: &T) -> Result<bool, Exception> {
    async {
        let conn = database.pool.get_with_timeout().await?;
        let sql = T::__upsert_sql();
        let params = entity.__insert_params();
        debug!("upsert, sql={sql}, params={params:?}");
        let row = conn.with_timeout(conn.client.query_one(sql, &params), database.query_timeout).await?;
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
        let conn = database.pool.get_with_timeout().await?;
        let sql = T::__insert_sql();
        let params = entity.__insert_params();
        debug!("insert, sql={sql}, params={params:?}");
        let row = conn.with_timeout(conn.client.query_one(sql, &params), database.query_timeout).await?;
        let id: i64 = row.try_get(0).map_err(|err| exception!(message = "failed to get result", source = err))?;
        debug!(db_write_rows = 1, "stats");
        Ok(id)
    }
    .instrument(debug_span!("db"))
    .await
}

pub async fn get<T>(database: &Database, ids: &T::Id) -> Result<Option<T>, Exception>
where
    T: Entity + FromRow,
{
    async {
        let mut conn = database.pool.get_with_timeout().await?;
        let sql = T::__get_sql();
        let params = T::__id_params(ids);
        debug!("get, sql={sql}, params={params:?}");
        let statement = conn.prepared_statement(sql).await?;
        let row = conn.with_timeout(conn.client.query_opt(&statement, &params), database.query_timeout).await?;
        debug!(db_read_rows = if row.is_some() { 1 } else { 0 }, "stats");
        row.map(T::try_from).transpose().map_err(|err| exception!(message = "failed to map row", source = err))
    }
    .instrument(debug_span!("db"))
    .await
}

// e.g. clause = "WHERE col = $1"
pub async fn select_one<T>(database: &Database, clause: &str, params: &[&QueryParam]) -> Result<Option<T>, Exception>
where
    T: Entity + FromRow,
{
    async {
        let mut conn = database.pool.get_with_timeout().await?;
        let sql = format!("{} {clause}", T::__select_sql());
        debug!("select_one, sql={sql}, params={params:?}");
        let statement = conn.prepared_statement(&sql).await?;
        let row = conn.with_timeout(conn.client.query_opt(&statement, params), database.query_timeout).await?;
        debug!(db_read_rows = if row.is_some() { 1 } else { 0 }, "stats");
        row.map(T::try_from).transpose().map_err(|err| exception!(message = "failed to map row", source = err))
    }
    .instrument(debug_span!("db"))
    .await
}

// e.g. clause = "WHERE col = $1"
pub async fn select<T>(database: &Database, clause: &str, params: &[&QueryParam]) -> Result<Vec<T>, Exception>
where
    T: Entity + FromRow,
{
    async {
        let mut conn = database.pool.get_with_timeout().await?;
        let sql = format!("{} {clause}", T::__select_sql());
        debug!("select, sql={sql}, params={params:?}");
        let statement = conn.prepared_statement(&sql).await?;
        let rows = conn.with_timeout(conn.client.query(&statement, params), database.query_timeout).await?;
        debug!(db_read_rows = rows.len(), "stats");
        rows.into_iter()
            .map(T::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| exception!(message = "failed to map row", source = err))
    }
    .instrument(debug_span!("db"))
    .await
}

pub async fn delete<T: Entity>(database: &Database, ids: &T::Id) -> Result<bool, Exception> {
    async {
        let mut conn = database.pool.get_with_timeout().await?;
        let sql = T::__delete_sql();
        let params = T::__id_params(ids);
        debug!("delete, sql={sql}, params={params:?}");
        let statement = conn.prepared_statement(sql).await?;
        let db_write_rows = conn.with_timeout(conn.client.execute(&statement, &params), database.query_timeout).await?;
        debug!(db_write_rows, "stats");
        Ok(db_write_rows != 0)
    }
    .instrument(debug_span!("db"))
    .await
}
