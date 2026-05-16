use tokio_postgres::types::ToSql;

use crate::Cond;
use crate::QueryParam;
use crate::Update;

pub trait Field {
    const COLUMN: &'static str;
    type Entity;
    type Value: ToSql + Sync + 'static;

    #[inline]
    fn update<'a>(&self, value: &'a Self::Value) -> Update<'a, Self::Entity> {
        Update::new(Self::COLUMN, value)
    }

    #[inline]
    fn eq<'a>(&self, value: &'a Self::Value) -> Cond<'a, Self::Entity> {
        Cond::eq(Self::COLUMN, value)
    }

    #[inline]
    fn is_in<'a>(&self, values: Vec<&'a Self::Value>) -> Cond<'a, Self::Entity> {
        Cond::is_in(Self::COLUMN, values.into_iter().map(|value| value as &QueryParam).collect())
    }

    #[inline]
    fn not_null(&self) -> Cond<'static, Self::Entity> {
        Cond::not_null(Self::COLUMN)
    }
}
