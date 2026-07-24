use std::marker::PhantomData;

use framework::write_str;
use tokio_postgres::types::ToSql;

use crate::QueryParam;

// `V` is the entity field type, so call sites pass an owned value, e.g. `FIELD_NAME.eq(name)` or
// `FIELD_NAME.eq("value".to_owned())`. A nullable column is set to NULL with `update(None)`.
pub struct Field<E, V> {
    column: &'static str,
    _marker: PhantomData<(E, V)>,
}

impl<E, V: ToSql + Sync + Send + 'static> Field<E, V> {
    pub const fn new(column: &'static str) -> Self {
        Field { column, _marker: PhantomData }
    }

    #[inline]
    pub const fn not_null(&self) -> Cond<E> {
        Cond { inner: CondInner::NotNull { column: self.column }, _entity: PhantomData }
    }

    #[inline]
    pub fn update(&self, value: V) -> Update<E> {
        Update { column: self.column, value: Box::new(value), _entity: PhantomData }
    }

    #[inline]
    pub fn eq(&self, value: V) -> Cond<E> {
        Cond { inner: CondInner::Eq { column: self.column, value: Box::new(value) }, _entity: PhantomData }
    }

    #[inline]
    pub fn is_in(&self, values: Vec<V>) -> Cond<E> {
        Cond {
            inner: CondInner::In {
                column: self.column,
                values: values.into_iter().map(|value| Box::new(value) as Box<dyn ToSql + Sync + Send>).collect(),
            },
            _entity: PhantomData,
        }
    }
}

pub struct Cond<E> {
    inner: CondInner,
    _entity: PhantomData<E>,
}

enum CondInner {
    Eq { column: &'static str, value: Box<dyn ToSql + Sync + Send> },
    In { column: &'static str, values: Vec<Box<dyn ToSql + Sync + Send>> },
    NotNull { column: &'static str },
}

pub(crate) fn build_conditions<'a, T>(
    conditions: &'a [Cond<T>],
    sql: &mut String,
    params: &mut Vec<&'a QueryParam>,
    param_index: &mut i32,
) {
    for (index, cond) in conditions.iter().enumerate() {
        if index == 0 {
            sql.push_str(" WHERE ");
        } else {
            sql.push_str(" AND ");
        }
        match cond.inner {
            CondInner::Eq { column, ref value } => {
                write_str!(sql, "{column} = ${param_index}");
                *param_index += 1;
                params.push(value.as_ref());
            }
            CondInner::In { column, ref values } => {
                write_str!(sql, "{column} IN (");
                for (i, _) in values.iter().enumerate() {
                    if i > 0 {
                        sql.push_str(", ");
                    }
                    write_str!(sql, "${param_index}");
                    *param_index += 1;
                }
                sql.push(')');

                params.extend(values.iter().map(|v| v.as_ref() as &QueryParam));
            }
            CondInner::NotNull { column } => {
                write_str!(sql, "{column} IS NOT NULL");
            }
        }
    }
}

pub struct Update<E> {
    column: &'static str,
    value: Box<dyn ToSql + Sync + Send>,
    _entity: PhantomData<E>,
}

pub(crate) fn build_update<'a, T>(
    updates: &'a [Update<T>],
    sql: &mut String,
    params: &mut Vec<&'a QueryParam>,
    param_index: &mut i32,
) {
    for (index, update) in updates.iter().enumerate() {
        if index > 0 {
            sql.push_str(", ");
        } else {
            sql.push_str(" SET ");
        }
        write_str!(sql, "{} = ${param_index}", update.column);
        *param_index += 1;
        params.push(update.value.as_ref());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct E;

    fn eq<V>(column: &'static str, value: V) -> Cond<E>
    where
        V: ToSql + Sync + Send + 'static,
    {
        Cond { inner: CondInner::Eq { column, value: Box::new(value) }, _entity: PhantomData }
    }

    fn is_in<V>(column: &'static str, values: Vec<V>) -> Cond<E>
    where
        V: ToSql + Sync + Send + 'static,
    {
        Cond {
            inner: CondInner::In {
                column,
                values: values.into_iter().map(|value| Box::new(value) as Box<dyn ToSql + Sync + Send>).collect(),
            },
            _entity: PhantomData,
        }
    }

    fn not_null(column: &'static str) -> Cond<E> {
        Cond { inner: CondInner::NotNull { column }, _entity: PhantomData }
    }

    fn update<V>(column: &'static str, value: V) -> Update<E>
    where
        V: ToSql + Sync + Send + 'static,
    {
        Update { column, value: Box::new(value), _entity: PhantomData }
    }

    #[test]
    fn build_conditions_empty() {
        let mut sql = String::from("SELECT 1");
        let mut params: Vec<&QueryParam> = vec![];
        let conditions = vec![];
        build_conditions::<E>(&conditions, &mut sql, &mut params, &mut 1);
        assert_eq!(sql, "SELECT 1");
        assert!(params.is_empty());
    }

    #[test]
    fn build_conditions_in() {
        let mut sql = String::from("SELECT 1");
        let mut params: Vec<&QueryParam> = vec![];
        let conditions = vec![is_in("id", vec![1, 2, 3])];
        build_conditions(&conditions, &mut sql, &mut params, &mut 1);
        assert_eq!(sql, "SELECT 1 WHERE id IN ($1, $2, $3)");
        assert_eq!(params.len(), 3);
    }

    #[test]
    fn build_conditions_multiple() {
        let mut sql = String::from("SELECT 1");
        let mut params: Vec<&QueryParam> = vec![];
        let conditions = vec![eq("id", 10), eq("name", "name"), not_null("deleted_at")];
        build_conditions(&conditions, &mut sql, &mut params, &mut 1);
        assert_eq!(sql, "SELECT 1 WHERE id = $1 AND name = $2 AND deleted_at IS NOT NULL");
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn build_update_and_conditions() {
        let mut sql = String::from("UPDATE t");
        let mut params: Vec<&QueryParam> = vec![];
        let mut index = 1;
        let updates = vec![update("col1", 99)];
        build_update(&updates, &mut sql, &mut params, &mut index);
        let conditions = vec![eq("id", 10)];
        build_conditions(&conditions, &mut sql, &mut params, &mut index);
        assert_eq!(sql, "UPDATE t SET col1 = $1 WHERE id = $2");
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn build_update_single() {
        let updates = vec![update("col1", 42)];
        let mut sql = String::from("UPDATE t");
        let mut params: Vec<&QueryParam> = vec![];
        let mut index = 1;
        build_update(&updates, &mut sql, &mut params, &mut index);
        assert_eq!(sql, "UPDATE t SET col1 = $1");
        assert_eq!(index, 2);
        assert_eq!(params.len(), 1);
    }

    #[test]
    fn build_update_multiple() {
        let updates = vec![update("col1", 1), update("col2", "value")];
        let mut sql = String::from("UPDATE t");
        let mut params: Vec<&QueryParam> = vec![];
        let mut index = 1;
        build_update(&updates, &mut sql, &mut params, &mut index);
        assert_eq!(sql, "UPDATE t SET col1 = $1, col2 = $2");
        assert_eq!(index, 3);
        assert_eq!(params.len(), 2);
    }
}
