use framework::exception;
use framework::exception::Exception;
use framework::log;
use framework::span;
use framework::stats;

use crate::Database;
use crate::Entity;
use crate::FromRow;
use crate::Insert;
use crate::InsertWithAutoIncrementId;
use crate::QueryParam;
use crate::field::Cond;
use crate::field::Update;
use crate::field::build_conditions;
use crate::field::build_update;

pub async fn insert<T: Insert>(database: &Database, entity: &T) -> Result<(), Exception> {
    let _span = span!("db");
    let conn = database.pool.get_with_timeout().await?;
    let sql = T::__insert_sql();
    let params = entity.__insert_params();
    log!("insert, sql={sql}, params={params:?}");
    let rows = conn.with_timeout(conn.client.execute(sql, &params), database.query_timeout).await?;
    stats!(db_write_rows = rows);
    Ok(())
}

// return true if inserted
pub async fn insert_ignore<T: Insert>(database: &Database, entity: &T) -> Result<bool, Exception> {
    let _span = span!("db");
    let conn = database.pool.get_with_timeout().await?;
    let sql = T::__insert_ignore_sql();
    let params = entity.__insert_params();
    log!("insert_ignore, sql={sql}, params={params:?}");
    let rows = conn.with_timeout(conn.client.execute(sql, &params), database.query_timeout).await?;
    stats!(db_write_rows = rows);
    Ok(rows != 0)
}

// return true if inserted
pub async fn upsert<T: Insert>(database: &Database, entity: &T) -> Result<bool, Exception> {
    let _span = span!("db");
    let conn = database.pool.get_with_timeout().await?;
    let sql = T::__upsert_sql();
    let params = entity.__insert_params();
    log!("upsert, sql={sql}, params={params:?}");
    let row = conn.with_timeout(conn.client.query_one(sql, &params), database.query_timeout).await?;
    let inserted: bool = row.try_get(0).map_err(|err| exception!("failed to get result", source = err))?;
    log!("inserted={inserted}");
    stats!(db_write_rows = 1); // postgres upsert always affects row
    Ok(inserted)
}

pub async fn insert_with_auto_increment_id<T: InsertWithAutoIncrementId>(
    database: &Database,
    entity: &T,
) -> Result<i64, Exception> {
    let _span = span!("db");
    let conn = database.pool.get_with_timeout().await?;
    let sql = T::__insert_sql();
    let params = entity.__insert_params();
    log!("insert, sql={sql}, params={params:?}");
    let row = conn.with_timeout(conn.client.query_one(sql, &params), database.query_timeout).await?;
    let id: i64 = row.try_get(0).map_err(|err| exception!("failed to get result", source = err))?;
    stats!(db_write_rows = 1);
    Ok(id)
}

pub async fn select_one<T>(database: &Database, conditions: Vec<Cond<T>>) -> Result<Option<T>, Exception>
where
    T: Entity + FromRow,
{
    let _span = span!("db");
    let mut conn = database.pool.get_with_timeout().await?;
    let mut sql = T::__select_sql().to_owned();
    let mut params: Vec<&QueryParam> = vec![];
    build_conditions(&conditions, &mut sql, &mut params, &mut 1);
    log!("select_one, sql={sql}, params={params:?}");
    let statement = conn.prepared_statement(&sql).await?;
    let row = conn.with_timeout(conn.client.query_opt(&statement, &params), database.query_timeout).await?;
    stats!(db_read_rows = if row.is_some() { 1 } else { 0 });
    row.map(T::try_from).transpose().map_err(|err| exception!("failed to map row", source = err))
}

pub async fn select_all<T>(database: &Database, conditions: Vec<Cond<T>>) -> Result<Vec<T>, Exception>
where
    T: Entity + FromRow,
{
    let _span = span!("db");
    let mut conn = database.pool.get_with_timeout().await?;
    let mut sql = T::__select_sql().to_owned();
    let mut params: Vec<&QueryParam> = vec![];
    build_conditions(&conditions, &mut sql, &mut params, &mut 1);
    log!("select, sql={sql}, params={params:?}");
    let statement = conn.prepared_statement(&sql).await?;
    let rows = conn.with_timeout(conn.client.query(&statement, &params), database.query_timeout).await?;
    stats!(db_read_rows = rows.len());
    rows.into_iter()
        .map(T::try_from)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| exception!("failed to map row", source = err))
}

pub async fn update<T: Entity>(
    database: &Database,
    updates: Vec<Update<T>>,
    conditions: Vec<Cond<T>>,
) -> Result<u64, Exception> {
    let _span = span!("db");
    let mut conn = database.pool.get_with_timeout().await?;
    let mut sql = format!("UPDATE \"{}\"", T::__table_name());
    let mut params: Vec<&QueryParam> = vec![];
    let mut param_index = 1;
    build_update(&updates, &mut sql, &mut params, &mut param_index);
    build_conditions(&conditions, &mut sql, &mut params, &mut param_index);
    log!("update, sql={sql}, params={params:?}");
    let statement = conn.prepared_statement(&sql).await?;
    let rows = conn.with_timeout(conn.client.execute(&statement, &params), database.query_timeout).await?;
    stats!(db_write_rows = rows);
    Ok(rows)
}

pub async fn delete<T: Entity>(database: &Database, conditions: Vec<Cond<T>>) -> Result<u64, Exception> {
    let _span = span!("db");
    let mut conn = database.pool.get_with_timeout().await?;
    let mut sql = format!("DELETE FROM \"{}\"", T::__table_name());
    let mut params: Vec<&QueryParam> = vec![];
    build_conditions(&conditions, &mut sql, &mut params, &mut 1);
    log!("delete, sql={sql}, params={params:?}");
    let statement = conn.prepared_statement(&sql).await?;
    let rows = conn.with_timeout(conn.client.execute(&statement, &params), database.query_timeout).await?;
    stats!(db_write_rows = rows);
    Ok(rows)
}
