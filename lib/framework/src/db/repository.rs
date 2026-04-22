use futures::TryFutureExt;
use tokio_postgres::Client;
use tokio_postgres::Row;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;

use crate::db::Database;
use crate::db::PgError;
use crate::db::QueryParam;
use crate::db::with_timeout;
use crate::exception;
use crate::exception::Exception;

#[allow(async_fn_in_trait)]
#[doc(hidden)] // disable auto complete, it's used by framework
pub trait InsertWithAutoIncrementId {
    async fn __insert(&self, client: &Client) -> Result<i64, PgError>;
}

#[allow(async_fn_in_trait)]
#[doc(hidden)] // disable auto complete, it's used by framework
pub trait Insert {
    async fn __insert(&self, client: &Client) -> Result<u64, PgError>;
    async fn __insert_ignore(&self, client: &Client) -> Result<u64, PgError>;
    async fn __upsert(&self, client: &Client) -> Result<bool, PgError>;
}

#[allow(async_fn_in_trait)]
#[doc(hidden)] // disable auto complete, it's used by framework
pub trait Select<T> {
    async fn __get(client: &Client, ids: &[&QueryParam]) -> Result<Option<T>, PgError>;
}

pub async fn insert(database: &Database, entity: &impl Insert) -> Result<(), Exception> {
    async {
        let connection = database.pool.get_with_timeout(database.connection_checkout_timeout).await?;

        let _: u64 = with_timeout(
            entity.__insert(&connection.client).map_err(|err| exception!(message = "failed to insert", source = err)),
            database.query_timeout,
            &connection.cancel_token,
        )
        .await?;

        debug!(db_write_rows = 1, "stats");
        Ok(())
    }
    .instrument(debug_span!("db"))
    .await
}

// return true if inserted
pub async fn insert_ignore(database: &Database, entity: &impl Insert) -> Result<bool, Exception> {
    async {
        let connection = database.pool.get_with_timeout(database.connection_checkout_timeout).await?;

        let updated_rows = with_timeout(
            entity
                .__insert_ignore(&connection.client)
                .map_err(|err| exception!(message = "failed to insert_ignore", source = err)),
            database.query_timeout,
            &connection.cancel_token,
        )
        .await?;

        debug!(db_write_rows = updated_rows, "stats");
        Ok(updated_rows == 1)
    }
    .instrument(debug_span!("db"))
    .await
}

// return true if inserted
pub async fn upsert(database: &Database, entity: &impl Insert) -> Result<bool, Exception> {
    async {
        let connection = database.pool.get_with_timeout(database.connection_checkout_timeout).await?;

        let inserted = with_timeout(
            entity.__upsert(&connection.client).map_err(|err| exception!(message = "failed to upsert", source = err)),
            database.query_timeout,
            &connection.cancel_token,
        )
        .await?;

        debug!("inserted={inserted}");
        debug!(db_write_rows = 1, "stats"); // postgres upsert always affects row
        Ok(inserted)
    }
    .instrument(debug_span!("db"))
    .await
}

pub async fn insert_with_auto_increment_id(
    database: &Database,
    entity: &impl InsertWithAutoIncrementId,
) -> Result<i64, Exception> {
    async {
        let connection = database.pool.get_with_timeout(database.connection_checkout_timeout).await?;

        let id = with_timeout(
            entity.__insert(&connection.client).map_err(|err| exception!(message = "failed to insert", source = err)),
            database.query_timeout,
            &connection.cancel_token,
        )
        .await?;

        debug!(db_write_rows = 1, "stats");
        Ok(id)
    }
    .instrument(debug_span!("db"))
    .await
}

pub async fn get<T>(database: &Database, ids: &[&QueryParam]) -> Result<Option<T>, Exception>
where
    T: Select<T> + TryFrom<Row, Error = PgError>,
{
    async {
        let connection = database.pool.get_with_timeout(database.connection_checkout_timeout).await?;
        let id = with_timeout(
            T::__get(&connection.client, ids).map_err(|err| exception!(message = "failed to select", source = err)),
            database.query_timeout,
            &connection.cancel_token,
        )
        .await?;

        let db_get_rows = if id.is_some() { 1 } else { 0 };
        debug!(db_get_rows, "stats");

        Ok(id)
    }
    .instrument(debug_span!("db"))
    .await
}
