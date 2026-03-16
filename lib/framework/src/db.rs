// use std::marker::PhantomData;
// use std::sync::Arc;

// use sqlx::Pool;
// use sqlx::Postgres;
// use sqlx::postgres::PgRow;

// pub struct Database {
//     pool: Pool<Postgres>,
// }

// pub struct Repository<T: Entity> {
//     db: Arc<Database>,
//     _marker: PhantomData<T>,
// }

// pub trait Entity {}

// pub trait Id {}

// impl Id for String {}

// impl<T: Entity> Repository<T> {
//     pub async fn get(&self, id: &dyn Id) -> Option<T> {
//         // let count: T = sqlx::query_as("")
//         //     .map(|row: PgRow| {
//         //         entity = T {};
//         //         return entity;
//         //     })
//         //     .fetch_optional(&self.db.pool)
//         //     .await
//         //     .unwrap();
//         None
//     }
// }
