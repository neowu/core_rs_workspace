use std::marker::PhantomData;

use framework::exception;
use framework::exception::Exception;
use framework::write_str;
use tokio_postgres::types::ToSql;

use crate::QueryParam;

pub struct Field<E, V> {
    column: &'static str,
    _marker: PhantomData<(E, V)>,
}

impl<E, V: ToSql + Sync + Send + 'static> Field<E, V> {
    #[doc(hidden)]
    pub const fn __new(column: &'static str) -> Self {
        Field { column, _marker: PhantomData }
    }

    #[inline]
    pub const fn is_null(&self) -> Cond<E> {
        Cond { column: self.column, inner: CondInner::IsNull, _entity: PhantomData }
    }

    #[inline]
    pub const fn not_null(&self) -> Cond<E> {
        Cond { column: self.column, inner: CondInner::NotNull, _entity: PhantomData }
    }

    #[inline]
    pub fn update(&self, value: V) -> Update<E> {
        Update { column: self.column, value: Box::new(value), _entity: PhantomData }
    }

    #[inline]
    pub fn eq(&self, value: V) -> Cond<E> {
        Cond { column: self.column, inner: CondInner::Eq(Box::new(value)), _entity: PhantomData }
    }

    #[inline]
    pub fn is_in(&self, values: Vec<V>) -> Result<Cond<E>, Exception> {
        if values.is_empty() {
            return Err(exception!(format!("is_in values must not be empty, column={}", self.column)));
        }
        Ok(Cond { column: self.column, inner: CondInner::In(Box::new(values)), _entity: PhantomData })
    }
}

pub struct Cond<E> {
    column: &'static str,
    inner: CondInner,
    _entity: PhantomData<E>,
}

enum CondInner {
    Eq(Box<dyn ToSql + Sync + Send + 'static>),
    // bind as one array param, so sql is same regardless of list size
    In(Box<dyn ToSql + Sync + Send + 'static>),
    IsNull,
    NotNull,
}

pub(crate) fn build_conditions<'a, T>(conditions: &'a [Cond<T>], sql: &mut String, params: &mut Vec<&'a QueryParam>) {
    for (index, cond) in conditions.iter().enumerate() {
        if index == 0 {
            sql.push_str(" WHERE ");
        } else {
            sql.push_str(" AND ");
        }
        let column = cond.column;
        match cond.inner {
            CondInner::Eq(ref value) => {
                params.push(value.as_ref());
                write_str!(sql, "{column} = ${}", params.len());
            }
            CondInner::In(ref values) => {
                params.push(values.as_ref());
                write_str!(sql, "{column} = ANY(${})", params.len());
            }
            CondInner::IsNull => {
                write_str!(sql, "{column} IS NULL");
            }
            CondInner::NotNull => {
                write_str!(sql, "{column} IS NOT NULL");
            }
        }
    }
}

pub struct Update<E> {
    column: &'static str,
    value: Box<dyn ToSql + Sync + Send + 'static>,
    _entity: PhantomData<E>,
}

pub(crate) fn build_update<'a, T>(updates: &'a [Update<T>], sql: &mut String, params: &mut Vec<&'a QueryParam>) {
    for (index, update) in updates.iter().enumerate() {
        if index > 0 {
            sql.push_str(", ");
        } else {
            sql.push_str(" SET ");
        }
        params.push(update.value.as_ref());
        write_str!(sql, "{} = ${}", update.column, params.len());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestEntity;

    impl TestEntity {
        const ID: Field<TestEntity, i64> = Field::__new("id");
        const COL1: Field<TestEntity, String> = Field::__new("col1");
        const COL2: Field<TestEntity, i32> = Field::__new("col2");
        const COL3: Field<TestEntity, Option<String>> = Field::__new("col3");
    }

    #[test]
    fn build_conditions_empty() {
        let mut sql = String::from("SELECT 1");
        let mut params: Vec<&QueryParam> = vec![];
        let conditions = vec![];
        build_conditions::<TestEntity>(&conditions, &mut sql, &mut params);
        assert_eq!(sql, "SELECT 1");
        assert!(params.is_empty());
    }

    #[test]
    fn build_conditions_in() {
        let mut sql = String::from("SELECT 1");
        let mut params: Vec<&QueryParam> = vec![];
        let conditions = vec![TestEntity::ID.is_in(vec![1, 2, 3]).unwrap()];
        build_conditions(&conditions, &mut sql, &mut params);
        assert_eq!(sql, "SELECT 1 WHERE id = ANY($1)");
        assert_eq!(params.len(), 1);
    }

    #[test]
    fn is_in_empty() {
        assert!(TestEntity::ID.is_in(vec![]).is_err());
    }

    #[test]
    fn build_conditions_multiple() {
        let mut sql = String::from("SELECT 1");
        let mut params: Vec<&QueryParam> = vec![];
        let conditions = vec![
            TestEntity::ID.eq(10),
            TestEntity::COL1.eq("value".to_owned()),
            TestEntity::COL3.not_null(),
            TestEntity::COL3.is_null(),
        ];
        build_conditions(&conditions, &mut sql, &mut params);
        assert_eq!(sql, "SELECT 1 WHERE id = $1 AND col1 = $2 AND col3 IS NOT NULL AND col3 IS NULL");
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn build_update_and_conditions() {
        let mut sql = String::from("UPDATE t");
        let mut params: Vec<&QueryParam> = vec![];
        let updates = vec![TestEntity::COL2.update(99)];
        build_update(&updates, &mut sql, &mut params);
        let conditions = vec![TestEntity::ID.eq(10)];
        build_conditions(&conditions, &mut sql, &mut params);
        assert_eq!(sql, "UPDATE t SET col2 = $1 WHERE id = $2");
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn build_update_single() {
        let updates = vec![TestEntity::COL2.update(42)];
        let mut sql = String::from("UPDATE t");
        let mut params: Vec<&QueryParam> = vec![];
        build_update(&updates, &mut sql, &mut params);
        assert_eq!(sql, "UPDATE t SET col2 = $1");
        assert_eq!(params.len(), 1);
    }

    #[test]
    fn build_update_multiple() {
        // a nullable column is set to NULL with update(None)
        let updates = vec![TestEntity::COL1.update("value".to_owned()), TestEntity::COL3.update(None)];
        let mut sql = String::from("UPDATE t");
        let mut params: Vec<&QueryParam> = vec![];
        build_update(&updates, &mut sql, &mut params);
        assert_eq!(sql, "UPDATE t SET col1 = $1, col3 = $2");
        assert_eq!(params.len(), 2);
    }
}
