use std::marker::PhantomData;

use framework::write_str;
use tokio_postgres::types::ToSql;

use crate::QueryParam;

pub struct Field<E, V> {
    column: &'static str,
    _marker: PhantomData<(E, V)>,
}

impl<E, V: ToSql + Sync + Send + 'static> Field<E, V> {
    pub const fn new(column: &'static str) -> Self {
        Field { column, _marker: PhantomData }
    }

    #[inline]
    pub fn update(&self, value: V) -> Update<E> {
        Update::new(self.column, value)
    }

    #[inline]
    pub fn eq<'a>(&self, value: &'a V) -> Cond<'a, E> {
        Cond { inner: CondInner::Eq { column: self.column, value }, _entity: PhantomData }
    }

    #[inline]
    pub fn is_in<'a>(&self, values: Vec<&'a V>) -> Cond<'a, E> {
        Cond {
            inner: CondInner::In {
                column: self.column,
                values: values.into_iter().map(|value| value as &QueryParam).collect(),
            },
            _entity: PhantomData,
        }
    }

    #[inline]
    pub const fn not_null(&self) -> Cond<'static, E> {
        Cond { inner: CondInner::NotNull { column: self.column }, _entity: PhantomData }
    }
}

pub struct Cond<'a, E> {
    inner: CondInner<'a>,
    _entity: PhantomData<E>,
}

enum CondInner<'a> {
    Eq { column: &'static str, value: &'a QueryParam },
    In { column: &'static str, values: Vec<&'a QueryParam> },
    NotNull { column: &'static str },
}

pub(crate) fn build_conditions<'a, T>(
    conditions: Vec<Cond<'a, T>>,
    sql: &mut String,
    params: &mut Vec<&'a QueryParam>,
    param_index: &mut i32,
) {
    for (index, cond) in conditions.into_iter().enumerate() {
        if index == 0 {
            sql.push_str(" WHERE ");
        } else {
            sql.push_str(" AND ");
        }
        match cond.inner {
            CondInner::Eq { column, value } => {
                write_str!(sql, "{column} = ${param_index}");
                *param_index += 1;
                params.push(value);
            }
            CondInner::In { column, values } => {
                write_str!(sql, "{column} IN (");
                for (i, _) in values.iter().enumerate() {
                    if i > 0 {
                        sql.push_str(", ");
                    }
                    write_str!(sql, "${param_index}");
                    *param_index += 1;
                }
                sql.push(')');
                params.extend(values);
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

impl<E> Update<E> {
    fn new<V: ToSql + Sync + Send + 'static>(column: &'static str, value: V) -> Update<E> {
        Self { column, value: Box::new(value), _entity: PhantomData }
    }
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

    fn eq<'a>(column: &'static str, value: &'a QueryParam) -> Cond<'a, E> {
        Cond { inner: CondInner::Eq { column, value }, _entity: PhantomData }
    }

    fn is_in<'a>(column: &'static str, values: Vec<&'a QueryParam>) -> Cond<'a, E> {
        Cond { inner: CondInner::In { column, values }, _entity: PhantomData }
    }

    fn not_null(column: &'static str) -> Cond<'static, E> {
        Cond { inner: CondInner::NotNull { column }, _entity: PhantomData }
    }

    #[test]
    fn build_conditions_empty() {
        let mut sql = String::from("SELECT 1");
        let mut params: Vec<&QueryParam> = vec![];
        build_conditions::<E>(vec![], &mut sql, &mut params, &mut 1);
        assert_eq!(sql, "SELECT 1");
        assert!(params.is_empty());
    }

    #[test]
    fn build_conditions_in() {
        let mut sql = String::from("SELECT 1");
        let mut params: Vec<&QueryParam> = vec![];
        build_conditions(vec![is_in("id", vec![&1 as &QueryParam, &2, &3])], &mut sql, &mut params, &mut 1);
        assert_eq!(sql, "SELECT 1 WHERE id IN ($1, $2, $3)");
        assert_eq!(params.len(), 3);
    }

    #[test]
    fn build_conditions_multiple() {
        let mut sql = String::from("SELECT 1");
        let mut params: Vec<&QueryParam> = vec![];
        build_conditions(
            vec![eq("id", &10), eq("name", &"name"), not_null("deleted_at")],
            &mut sql,
            &mut params,
            &mut 1,
        );
        assert_eq!(sql, "SELECT 1 WHERE id = $1 AND name = $2 AND deleted_at IS NOT NULL");
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn build_update_and_conditions() {
        let mut sql = String::from("UPDATE t");
        let mut params: Vec<&QueryParam> = vec![];
        let mut index = 1;
        let updates = vec![Update::<E>::new("col1", 99)];
        build_update(&updates, &mut sql, &mut params, &mut index);
        build_conditions(vec![eq("id", &10)], &mut sql, &mut params, &mut index);
        assert_eq!(sql, "UPDATE t SET col1 = $1 WHERE id = $2");
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn build_update_single() {
        let updates = vec![Update::<E>::new("col1", 42)];
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
        let updates = vec![Update::<E>::new("col1", 1), Update::<E>::new("col2", "value")];
        let mut sql = String::from("UPDATE t");
        let mut params: Vec<&QueryParam> = vec![];
        let mut index = 1;
        build_update(&updates, &mut sql, &mut params, &mut index);
        assert_eq!(sql, "UPDATE t SET col1 = $1, col2 = $2");
        assert_eq!(index, 3);
        assert_eq!(params.len(), 2);
    }
}
