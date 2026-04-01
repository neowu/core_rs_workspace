use std::marker::PhantomData;
use std::sync::Arc;

use sqlx::FromRow;
use sqlx::Pool;
use sqlx::Postgres;
use sqlx::postgres::PgRow;

pub struct Database {
    pub pool: Pool<Postgres>,
}

pub struct Repository<T>
where
    T: Entity + for<'a> FromRow<'a, PgRow>,
{
    pub db: Arc<Database>,
    pub _marker: PhantomData<T>,
}

pub trait Entity {
    fn select_sql() -> String;
}

pub trait Id {
    fn to_string(&self) -> String;
}

impl Id for String {
    fn to_string(&self) -> String {
        self.clone()
    }
}

impl<T> Repository<T>
where
    T: Entity + for<'a> FromRow<'a, PgRow> + Send + Sync + Unpin,
{
    pub async fn get(&self, id: impl Id) -> Option<T> {
        let sql = T::select_sql();
        sqlx::query_as(&sql)
            .bind(id.to_string())
            .fetch_optional(&self.db.pool)
            .await
            .unwrap()
    }
}
