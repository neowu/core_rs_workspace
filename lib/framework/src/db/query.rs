use std::marker::PhantomData;

use crate::db::QueryParam;
use crate::write_str;

pub struct Update<'a, E> {
    column: &'static str,
    value: &'a QueryParam,
    _entity: PhantomData<E>,
}

impl<'a, E> Update<'a, E> {
    pub(super) fn new(column: &'static str, value: &'a QueryParam) -> Update<'a, E> {
        Self { column, value, _entity: PhantomData }
    }
}

pub(super) fn build_update<'a, T>(
    updates: Vec<Update<'a, T>>,
    sql: &mut String,
    params: &mut Vec<&'a QueryParam>,
    param_index: &mut i32,
) {
    for (index, update) in updates.into_iter().enumerate() {
        if index > 0 {
            sql.push_str(", ");
        } else {
            sql.push_str(" SET ");
        }
        write_str!(sql, "{} = ${param_index}", update.column);
        *param_index += 1;
        params.push(update.value);
    }
}

pub enum Cond<'a, E> {
    Eq { column: &'static str, value: &'a QueryParam, _entity: PhantomData<E> },
    In { column: &'static str, values: Vec<&'a QueryParam>, _entity: PhantomData<E> },
    NotNull { column: &'static str, _entity: PhantomData<E> },
}

impl<'a, E> Cond<'a, E> {
    pub(super) fn eq(column: &'static str, value: &'a QueryParam) -> Self {
        Cond::Eq { column, value, _entity: PhantomData }
    }

    pub(super) fn is_in(column: &'static str, values: Vec<&'a QueryParam>) -> Self {
        Cond::In { column, values, _entity: PhantomData }
    }

    pub(super) fn not_null(column: &'static str) -> Cond<'static, E> {
        Cond::NotNull { column, _entity: PhantomData }
    }
}

pub(super) fn build_conditions<'a, T>(
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
        match cond {
            Cond::Eq { column, value, .. } => {
                write_str!(sql, "{column} = ${param_index}");
                *param_index += 1;
                params.push(value);
            }
            Cond::In { column, values, .. } => {
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
            Cond::NotNull { column, .. } => {
                write_str!(sql, "{column} IS NOT NULL");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct E;

    #[test]
    fn build_update_single() {
        let updates = vec![Update::new("col1", &42 as &QueryParam)];
        let mut sql = String::from("UPDATE t");
        let mut params: Vec<&QueryParam> = vec![];
        let mut index = 1;
        build_update::<E>(updates, &mut sql, &mut params, &mut index);
        assert_eq!(sql, "UPDATE t SET col1 = $1");
        assert_eq!(index, 2);
        assert_eq!(params.len(), 1);
    }

    #[test]
    fn build_update_multiple() {
        let updates = vec![Update::new("col1", &1), Update::new("col2", &&"value")];
        let mut sql = String::from("UPDATE t");
        let mut params: Vec<&QueryParam> = vec![];
        let mut index = 1;
        build_update::<E>(updates, &mut sql, &mut params, &mut index);
        assert_eq!(sql, "UPDATE t SET col1 = $1, col2 = $2");
        assert_eq!(index, 3);
        assert_eq!(params.len(), 2);
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
        build_conditions(vec![Cond::<E>::is_in("id", vec![&1 as &QueryParam, &2, &3])], &mut sql, &mut params, &mut 1);
        assert_eq!(sql, "SELECT 1 WHERE id IN ($1, $2, $3)");
        assert_eq!(params.len(), 3);
    }

    #[test]
    fn build_conditions_multiple() {
        let mut sql = String::from("SELECT 1");
        let mut params: Vec<&QueryParam> = vec![];
        build_conditions(
            vec![Cond::<E>::eq("id", &10), Cond::<E>::eq("name", &"name"), Cond::<E>::not_null("deleted_at")],
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
        build_update::<E>(vec![Update::new("col1", &99)], &mut sql, &mut params, &mut index);
        build_conditions(vec![Cond::<E>::eq("id", &10)], &mut sql, &mut params, &mut index);
        assert_eq!(sql, "UPDATE t SET col1 = $1 WHERE id = $2");
        assert_eq!(params.len(), 2);
    }
}
