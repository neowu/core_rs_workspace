use std::marker::PhantomData;

use framework::write_str;

use crate::QueryParam;

pub struct Update<'a, E> {
    column: &'static str,
    value: &'a QueryParam,
    _entity: PhantomData<E>,
}

impl<'a, E> Update<'a, E> {
    pub(crate) fn new(column: &'static str, value: &'a QueryParam) -> Update<'a, E> {
        Self { column, value, _entity: PhantomData }
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
        write_str!(sql, "{} = ${param_index}", update.column);
        *param_index += 1;
        params.push(update.value);
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
}
