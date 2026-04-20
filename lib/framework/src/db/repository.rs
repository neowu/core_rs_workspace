use futures::TryFutureExt;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;

use crate::db::Database;
use crate::db::Insert;
use crate::db::InsertWithAutoIncrementId;
use crate::db::with_timeout;
use crate::exception;
use crate::exception::Exception;

pub async fn insert(database: &Database, entity: &impl Insert) -> Result<(), Exception> {
    let span = debug_span!("db");
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
    .instrument(span)
    .await
}

// return true if inserted
pub async fn insert_ignore(database: &Database, entity: &impl Insert) -> Result<bool, Exception> {
    let span = debug_span!("db");
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
    .instrument(span)
    .await
}

// return true if inserted
pub async fn upsert(database: &Database, entity: &impl Insert) -> Result<bool, Exception> {
    let span = debug_span!("db");
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
    .instrument(span)
    .await
}

pub async fn insert_with_auto_increment_id(
    database: &Database,
    entity: &impl InsertWithAutoIncrementId,
) -> Result<i64, Exception> {
    let span = debug_span!("db");
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
    .instrument(span)
    .await
}
