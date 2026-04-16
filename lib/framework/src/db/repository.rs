use crate::db::Database;
use crate::db::Insert;
use crate::db::InsertWithAutoIncrementId;
use crate::exception;
use crate::exception::Exception;

pub async fn insert(database: &Database, entity: &impl Insert) -> Result<(), Exception> {
    let connection = database
        .pool
        .get_with_timeout(database.connection_checkout_timeout)
        .await?;

    entity
        .__insert(&connection.client)
        .await
        .map_err(|err| exception!(message = "failed to insert", source = err))?;

    Ok(())
}

pub async fn insert_with_auto_increment_id(
    database: &Database,
    entity: &impl InsertWithAutoIncrementId,
) -> Result<i64, Exception> {
    let connection = database
        .pool
        .get_with_timeout(database.connection_checkout_timeout)
        .await?;

    let row = entity
        .__insert(&connection.client)
        .await
        .map_err(|err| exception!(message = "failed to insert", source = err))?;
    Ok(row.get(0))
}
