use std::marker::PhantomData;

use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;

use crate::db::Database;
use crate::db::FromRow;
use crate::db::QueryParam;
use crate::db::ToSql;
use crate::exception;
use crate::exception::Exception;

pub struct Update<E> {
    column: &'static str,
    value: Box<QueryParam>,
    _entity: PhantomData<E>,
}

impl<E> Update<E> {
    fn build_query(self, sql: &mut String, index: &mut i32, params: &mut Vec<Box<QueryParam>>) {
        sql.push_str(&format!("{} = ${}", self.column, index));
        *index += 1;
        params.push(self.value);
    }
}

pub enum Cond<E> {
    Eq { column: &'static str, value: Box<QueryParam>, _entity: PhantomData<E> },
    In { column: &'static str, values: Vec<Box<QueryParam>>, _entity: PhantomData<E> },
}

impl<E> Cond<E> {
    fn build_query(self, sql: &mut String, index: &mut i32, params: &mut Vec<Box<QueryParam>>) {
        match self {
            Cond::Eq { column, value, .. } => {
                sql.push_str(&format!("{column} = ${index}"));
                *index += 1;
                params.push(value);
            }
            Cond::In { column, values, .. } => {
                sql.push_str(column);
                sql.push_str(" IN (");
                sql.push_str(
                    &(0..values.len())
                        .map(|_| {
                            let placeholder = format!("${index}");
                            *index += 1;
                            placeholder
                        })
                        .collect::<Vec<_>>()
                        .join(", "),
                );
                sql.push(')');
                params.extend(values);
            }
        }
    }
}

pub trait Field {
    const COLUMN: &'static str;
    type Value: ToSql + Sync + 'static;
    type Entity;

    fn update(value: Self::Value) -> Update<Self::Entity> {
        Update { column: Self::COLUMN, value: Box::new(value), _entity: PhantomData }
    }

    fn eq(value: Self::Value) -> Cond<Self::Entity> {
        Cond::Eq { column: Self::COLUMN, value: Box::new(value), _entity: PhantomData }
    }

    fn is_in(values: Vec<Self::Value>) -> Cond<Self::Entity> {
        Cond::In {
            column: Self::COLUMN,
            values: values.into_iter().map(|value| Box::new(value) as Box<QueryParam>).collect(),
            _entity: PhantomData,
        }
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
    fn __id_params(ids: &Self::Id) -> Vec<&QueryParam>;
    fn __table_name() -> &'static str;
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

pub async fn select_one<T>(database: &Database, conditions: Vec<Cond<T>>) -> Result<Option<T>, Exception>
where
    T: Entity + FromRow,
{
    async {
        let mut conn = database.pool.get_with_timeout().await?;
        let mut sql = T::__select_sql().to_string();
        let mut params: Vec<Box<QueryParam>> = vec![];
        let mut param_index = 1;
        for (index, cond) in conditions.into_iter().enumerate() {
            if index == 0 {
                sql.push_str(" WHERE ");
            } else {
                sql.push_str(" AND ");
            }
            cond.build_query(&mut sql, &mut param_index, &mut params);
        }
        debug!("select_one, sql={sql}, params={params:?}");
        let statement = conn.prepared_statement(&sql).await?;
        let param_refs: Vec<&QueryParam> = params.iter().map(|p| p.as_ref()).collect();
        let row = conn.with_timeout(conn.client.query_opt(&statement, &param_refs), database.query_timeout).await?;
        debug!(db_read_rows = if row.is_some() { 1 } else { 0 }, "stats");
        row.map(T::try_from).transpose().map_err(|err| exception!(message = "failed to map row", source = err))
    }
    .instrument(debug_span!("db"))
    .await
}

pub async fn select_all<T>(database: &Database, conditions: Vec<Cond<T>>) -> Result<Vec<T>, Exception>
where
    T: Entity + FromRow,
{
    async {
        let mut conn = database.pool.get_with_timeout().await?;
        let mut sql = T::__select_sql().to_string();
        let mut params: Vec<Box<QueryParam>> = vec![];
        let mut param_index = 1;
        for (index, cond) in conditions.into_iter().enumerate() {
            if index == 0 {
                sql.push_str(" WHERE ");
            } else {
                sql.push_str(" AND ");
            }
            cond.build_query(&mut sql, &mut param_index, &mut params);
        }
        debug!("select, sql={sql}, params={params:?}");
        let statement = conn.prepared_statement(&sql).await?;
        let param_refs: Vec<&QueryParam> = params.iter().map(|p| p.as_ref()).collect();
        let rows = conn.with_timeout(conn.client.query(&statement, &param_refs), database.query_timeout).await?;
        debug!(db_read_rows = rows.len(), "stats");
        rows.into_iter()
            .map(T::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| exception!(message = "failed to map row", source = err))
    }
    .instrument(debug_span!("db"))
    .await
}

pub async fn update_all<T: Entity>(
    database: &Database,
    updates: Vec<Update<T>>,
    conditions: Vec<Cond<T>>,
) -> Result<u64, Exception> {
    async {
        let mut conn = database.pool.get_with_timeout().await?;
        let mut sql = format!("UPDATE \"{}\" SET ", T::__table_name());
        let mut params: Vec<Box<QueryParam>> = vec![];
        let mut param_index = 1;
        for (index, u) in updates.into_iter().enumerate() {
            if index > 0 {
                sql.push_str(", ");
            }
            u.build_query(&mut sql, &mut param_index, &mut params);
        }
        for (index, c) in conditions.into_iter().enumerate() {
            if index == 0 {
                sql.push_str(" WHERE ");
            } else {
                sql.push_str(" AND ");
            }
            c.build_query(&mut sql, &mut param_index, &mut params);
        }
        debug!("update_all, sql={sql}, params={params:?}");
        let param_refs: Vec<&QueryParam> = params.iter().map(|p| p.as_ref()).collect();
        let statement = conn.prepared_statement(&sql).await?;
        let db_write_rows =
            conn.with_timeout(conn.client.execute(&statement, &param_refs), database.query_timeout).await?;
        debug!(db_write_rows, "stats");
        Ok(db_write_rows)
    }
    .instrument(debug_span!("db"))
    .await
}

pub async fn delete<T: Entity>(database: &Database, id: &T::Id) -> Result<bool, Exception> {
    async {
        let mut conn = database.pool.get_with_timeout().await?;
        let sql = T::__delete_sql();
        let params = T::__id_params(id);
        debug!("delete, sql={sql}, params={params:?}");
        let statement = conn.prepared_statement(sql).await?;
        let db_write_rows = conn.with_timeout(conn.client.execute(&statement, &params), database.query_timeout).await?;
        debug!(db_write_rows, "stats");
        Ok(db_write_rows != 0)
    }
    .instrument(debug_span!("db"))
    .await
}
