use futures::TryFutureExt;
use tokio_postgres::Row;
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
        let connection = database
            .pool
            .get_with_timeout(database.connection_checkout_timeout)
            .await?;

        let _: u64 = with_timeout(
            entity
                .__insert(&connection.client)
                .map_err(|err| exception!(message = "failed to insert", source = err)),
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

pub async fn insert_with_auto_increment_id(
    database: &Database,
    entity: &impl InsertWithAutoIncrementId,
) -> Result<i64, Exception> {
    let span = debug_span!("db");
    async {
        let connection = database
            .pool
            .get_with_timeout(database.connection_checkout_timeout)
            .await?;

        let row: Row = with_timeout(
            entity
                .__insert(&connection.client)
                .map_err(|err| exception!(message = "failed to insert", source = err)),
            database.query_timeout,
            &connection.cancel_token,
        )
        .await?;

        debug!(db_write_rows = 1, "stats");
        Ok(row.get(0))
    }
    .instrument(span)
    .await
}
