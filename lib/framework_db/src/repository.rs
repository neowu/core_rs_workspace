use framework::exception;
use framework::exception::Exception;
use framework::stats;
use tracing::Instrument as _;
use tracing::debug;
use tracing::debug_span;

use crate::Database;
use crate::Entity;
use crate::FromRow;
use crate::Insert;
use crate::InsertWithAutoIncrementId;
use crate::QueryParam;
use crate::cond::Cond;
use crate::cond::build_conditions;
use crate::update::Update;
use crate::update::build_update;

pub async fn insert<T: Insert>(database: &Database, entity: &T) -> Result<(), Exception> {
    async {
        let conn = database.pool.get_with_timeout().await?;
        let sql = T::__insert_sql();
        let params = entity.__insert_params();
        debug!("insert, sql={sql}, params={params:?}");
        let db_write_rows = conn.with_timeout(conn.client.execute(sql, &params), database.query_timeout).await?;
        stats!(db_write_rows);
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
        stats!(db_write_rows);
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
        let inserted: bool = row.try_get(0).map_err(|err| exception!("failed to get result", source = err))?;
        debug!("inserted={inserted}");
        stats!(db_write_rows = 1); // postgres upsert always affects row
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
        let id: i64 = row.try_get(0).map_err(|err| exception!("failed to get result", source = err))?;
        stats!(db_write_rows = 1);
        Ok(id)
    }
    .instrument(debug_span!("db"))
    .await
}

pub async fn get<T>(database: &Database, id: &T::Id) -> Result<Option<T>, Exception>
where
    T: Entity<Type = T> + FromRow,
{
    select_one(database, T::__id_conditions(id)).await
}

pub async fn select_one<T>(database: &Database, conditions: Vec<Cond<'_, T>>) -> Result<Option<T>, Exception>
where
    T: Entity + FromRow,
{
    async {
        let mut conn = database.pool.get_with_timeout().await?;
        let mut sql = T::__select_sql().to_owned();
        let mut params: Vec<&QueryParam> = vec![];
        build_conditions(conditions, &mut sql, &mut params, &mut 1);
        debug!("select_one, sql={sql}, params={params:?}");
        let statement = conn.prepared_statement(&sql).await?;
        let row = conn.with_timeout(conn.client.query_opt(&statement, &params), database.query_timeout).await?;
        stats!(db_read_rows = if row.is_some() { 1 } else { 0 });
        row.map(T::try_from).transpose().map_err(|err| exception!("failed to map row", source = err))
    }
    .instrument(debug_span!("db"))
    .await
}

pub async fn select_all<T>(database: &Database, conditions: Vec<Cond<'_, T>>) -> Result<Vec<T>, Exception>
where
    T: Entity + FromRow,
{
    async {
        let mut conn = database.pool.get_with_timeout().await?;
        let mut sql = T::__select_sql().to_owned();
        let mut params: Vec<&QueryParam> = vec![];
        build_conditions(conditions, &mut sql, &mut params, &mut 1);
        debug!("select, sql={sql}, params={params:?}");
        let statement = conn.prepared_statement(&sql).await?;
        let rows = conn.with_timeout(conn.client.query(&statement, &params), database.query_timeout).await?;
        stats!(db_read_rows = rows.len());
        rows.into_iter()
            .map(T::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| exception!("failed to map row", source = err))
    }
    .instrument(debug_span!("db"))
    .await
}

pub async fn update_with_condition<T: Entity<Type = T>>(
    database: &Database,
    id: &T::Id,
    updates: Vec<Update<T>>,
    mut conditions: Vec<Cond<'_, T>>,
) -> Result<bool, Exception> {
    async {
        let mut conn = database.pool.get_with_timeout().await?;
        let mut sql = format!("UPDATE \"{}\"", T::__table_name());
        let mut params: Vec<&QueryParam> = vec![];
        let mut param_index = 1;
        build_update(&updates, &mut sql, &mut params, &mut param_index);
        conditions.extend(T::__id_conditions(id));
        build_conditions(conditions, &mut sql, &mut params, &mut param_index);
        debug!("update, sql={sql}, params={params:?}");
        let statement = conn.prepared_statement(&sql).await?;
        let db_write_rows = conn.with_timeout(conn.client.execute(&statement, &params), database.query_timeout).await?;
        stats!(db_write_rows);
        Ok(db_write_rows == 1)
    }
    .instrument(debug_span!("db"))
    .await
}

pub async fn update<T: Entity<Type = T>>(
    database: &Database,
    id: &T::Id,
    updates: Vec<Update<T>>,
) -> Result<bool, Exception> {
    update_with_condition(database, id, updates, vec![]).await
}

pub async fn update_all<T: Entity>(
    database: &Database,
    updates: Vec<Update<T>>,
    conditions: Vec<Cond<'_, T>>,
) -> Result<u64, Exception> {
    async {
        let mut conn = database.pool.get_with_timeout().await?;
        let mut sql = format!("UPDATE \"{}\"", T::__table_name());
        let mut params: Vec<&QueryParam> = vec![];
        let mut param_index = 1;
        build_update(&updates, &mut sql, &mut params, &mut param_index);
        build_conditions(conditions, &mut sql, &mut params, &mut param_index);
        debug!("update_all, sql={sql}, params={params:?}");
        let statement = conn.prepared_statement(&sql).await?;
        let db_write_rows = conn.with_timeout(conn.client.execute(&statement, &params), database.query_timeout).await?;
        stats!(db_write_rows);
        Ok(db_write_rows)
    }
    .instrument(debug_span!("db"))
    .await
}

pub async fn delete<T: Entity>(database: &Database, id: &T::Id) -> Result<bool, Exception> {
    async {
        let mut conn = database.pool.get_with_timeout().await?;
        let mut sql = format!("DELETE FROM \"{}\"", T::__table_name());
        let mut params: Vec<&QueryParam> = vec![];
        build_conditions(T::__id_conditions(id), &mut sql, &mut params, &mut 1);
        debug!("delete, sql={sql}, params={params:?}");
        let statement = conn.prepared_statement(&sql).await?;
        let db_write_rows = conn.with_timeout(conn.client.execute(&statement, &params), database.query_timeout).await?;
        stats!(db_write_rows);
        Ok(db_write_rows != 0)
    }
    .instrument(debug_span!("db"))
    .await
}

pub async fn delete_all<T: Entity>(database: &Database, conditions: Vec<Cond<'_, T>>) -> Result<u64, Exception> {
    async {
        let mut conn = database.pool.get_with_timeout().await?;
        let mut sql = format!("DELETE FROM \"{}\"", T::__table_name());
        let mut params: Vec<&QueryParam> = vec![];
        build_conditions(conditions, &mut sql, &mut params, &mut 1);
        debug!("delete_all, sql={sql}, params={params:?}");
        let statement = conn.prepared_statement(&sql).await?;
        let db_write_rows = conn.with_timeout(conn.client.execute(&statement, &params), database.query_timeout).await?;
        stats!(db_write_rows);
        Ok(db_write_rows)
    }
    .instrument(debug_span!("db"))
    .await
}
