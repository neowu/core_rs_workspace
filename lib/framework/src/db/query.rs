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
