use std::marker::PhantomData;

use crate::db::QueryParam;

pub struct Update<'a, E> {
    column: &'static str,
    value: &'a QueryParam,
    _entity: PhantomData<E>,
}

impl<'a, E> Update<'a, E> {
    pub(crate) fn new(column: &'static str, value: &'a QueryParam) -> Update<'a, E> {
        Self { column, value, _entity: PhantomData }
    }

    fn build_query(&self, sql: &mut String, index: &mut i32, params: &mut Vec<&'a QueryParam>) {
        sql.push_str(&format!("{} = ${}", self.column, index));
        *index += 1;
        params.push(self.value);
    }
}

pub(crate) fn build_update<'a, T>(
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
        update.build_query(sql, param_index, params);
    }
}

pub enum Cond<'a, E> {
    Eq { column: &'static str, value: &'a QueryParam, _entity: PhantomData<E> },
    In { column: &'static str, values: Vec<&'a QueryParam>, _entity: PhantomData<E> },
    NotNull { column: &'static str, _entity: PhantomData<E> },
}

impl<'a, E> Cond<'a, E> {
    fn build_query(&self, sql: &mut String, index: &mut i32, params: &mut Vec<&'a QueryParam>) {
        match self {
            Cond::Eq { column, value, .. } => {
                sql.push_str(&format!("{column} = ${index}"));
                *index += 1;
                params.push(*value);
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
            Cond::NotNull { column, _entity } => {
                sql.push_str(column);
                sql.push_str(" IS NOT NULL");
            }
        }
    }
}

pub(crate) fn build_conditions<'a, T>(
    conditions: Vec<Cond<'a, T>>,
    sql: &mut String,
    params: &mut Vec<&'a QueryParam>,
    mut param_index: i32,
) {
    for (index, cond) in conditions.into_iter().enumerate() {
        if index == 0 {
            sql.push_str(" WHERE ");
        } else {
            sql.push_str(" AND ");
        }
        cond.build_query(sql, &mut param_index, params);
    }
}

#[cfg(test)]
mod tests {
    use std::marker::PhantomData;

    use super::*;

    struct Entity;

    #[test]
    fn test_build_update_single() {
        let updates = vec![Update::new("col1", &42 as &QueryParam)];
        let mut sql = String::from("UPDATE t");
        let mut params: Vec<&QueryParam> = vec![];
        let mut index = 1;
        build_update::<Entity>(updates, &mut sql, &mut params, &mut index);
        assert_eq!(sql, "UPDATE t SET col1 = $1");
        assert_eq!(index, 2);
        assert_eq!(params.len(), 1);
    }

    #[test]
    fn test_build_update_multiple() {
        let updates = vec![Update::new("col1", &1), Update::new("col2", &&"value")];
        let mut sql = String::from("UPDATE t");
        let mut params: Vec<&QueryParam> = vec![];
        let mut index = 1;
        build_update::<Entity>(updates, &mut sql, &mut params, &mut index);
        assert_eq!(sql, "UPDATE t SET col1 = $1, col2 = $2");
        assert_eq!(index, 3);
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn test_build_conditions_empty() {
        let mut sql = String::from("SELECT 1");
        let mut params: Vec<&QueryParam> = vec![];
        build_conditions::<Entity>(vec![], &mut sql, &mut params, 1);
        assert_eq!(sql, "SELECT 1");
        assert!(params.is_empty());
    }

    #[test]
    fn test_build_conditions_in() {
        let mut sql = String::from("SELECT 1");
        let mut params: Vec<&QueryParam> = vec![];
        build_conditions::<Entity>(
            vec![Cond::In { column: "id", values: vec![&1 as &QueryParam, &2, &3], _entity: PhantomData }],
            &mut sql,
            &mut params,
            1,
        );
        assert_eq!(sql, "SELECT 1 WHERE id IN ($1, $2, $3)");
        assert_eq!(params.len(), 3);
    }

    #[test]
    fn test_build_conditions() {
        let mut sql = String::from("SELECT 1");
        let mut params: Vec<&QueryParam> = vec![];
        build_conditions::<Entity>(
            vec![
                Cond::Eq { column: "id", value: &10, _entity: PhantomData },
                Cond::Eq { column: "name", value: &"name", _entity: PhantomData },
                Cond::NotNull { column: "deleted_at", _entity: PhantomData },
            ],
            &mut sql,
            &mut params,
            1,
        );
        assert_eq!(sql, "SELECT 1 WHERE id = $1 AND name = $2 AND deleted_at IS NOT NULL");
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn test_build_update_and_conditions() {
        let mut sql = String::from("UPDATE t");
        let mut params: Vec<&QueryParam> = vec![];
        let mut index = 1;
        build_update::<Entity>(vec![Update::new("col1", &99)], &mut sql, &mut params, &mut index);
        build_conditions::<Entity>(
            vec![Cond::Eq { column: "id", value: &10, _entity: PhantomData }],
            &mut sql,
            &mut params,
            index,
        );
        assert_eq!(sql, "UPDATE t SET col1 = $1 WHERE id = $2");
        assert_eq!(params.len(), 2);
    }
}
