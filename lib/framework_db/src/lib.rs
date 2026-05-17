use std::sync::Arc;
use std::time::Duration;

pub use cond::Cond;
pub use field::Field;
use framework::pool::ResourcePool;
pub use tokio_postgres::Config;
pub use tokio_postgres::Error as PgError;
pub use tokio_postgres::Row;
use tokio_postgres::types::FromSqlOwned;
pub use tokio_postgres::types::ToSql;
pub use update::Update;

use crate::connection::ConnectionManager;

mod cond;
mod connection;
pub mod database;
mod field;
pub mod repository;
mod update;

pub type QueryParam = dyn ToSql + Sync;

pub trait FromRow: Sized {
    fn try_from(row: Row) -> Result<Self, PgError>;
}

impl<T> FromRow for T
where
    T: FromSqlOwned,
{
    fn try_from(row: Row) -> Result<Self, PgError> {
        row.try_get(0)
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
    type Type;
    fn __id_conditions(ids: &Self::Id) -> Vec<Cond<'_, Self::Type>>;
    fn __table_name() -> &'static str;
    fn __select_sql() -> &'static str;
}

pub struct Database {
    pool: Arc<ResourcePool<ConnectionManager>>,
    query_timeout: Duration,
}

impl Database {
    pub fn new(config: Config) -> Self {
        let pool = Arc::new(ResourcePool::new(
            ConnectionManager { config },
            50,
            Duration::from_secs(30),
            Duration::from_hours(1),
            Duration::from_secs(5),
        ));
        Database { pool, query_timeout: Duration::from_secs(5) }
    }
}
