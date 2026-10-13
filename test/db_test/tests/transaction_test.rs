use std::time::Duration;

use db_test::client;
use framework::exception::Exception;
use framework_db::Database;
use framework_db::database;
use framework_db::repository;
use framework_macro::Entity;
use framework_macro::integration_test;

#[derive(Entity, Debug, PartialEq)]
#[table(name = "transaction_entity")]
struct TransactionEntity {
    #[primary_key]
    #[column(name = "id")]
    id: i64,
    #[column(name = "name")]
    name: String,
}

async fn setup_schema(db: &Database) -> Result<(), Exception> {
    database::execute(db, "DROP TABLE IF EXISTS \"transaction_entity\"", &[]).await?;
    database::execute(db, "CREATE TABLE \"transaction_entity\" (id BIGINT PRIMARY KEY, name TEXT NOT NULL)", &[])
        .await?;
    Ok(())
}

fn entity(id: i64) -> TransactionEntity {
    TransactionEntity { id, name: format!("name-{id}") }
}

async fn count(db: &Database) -> Result<i64, Exception> {
    Ok(database::select_one(db, "SELECT count(*) FROM \"transaction_entity\"", &[]).await?.unwrap_or(0))
}

#[integration_test]
async fn transaction() -> Result<(), Exception> {
    let db = client();
    setup_schema(&db).await?;

    // commit persists all statements
    let mut tx = db.begin().await?;
    repository::insert(&mut tx, &entity(1)).await?;
    repository::insert(&mut tx, &entity(2)).await?;
    assert_eq!(repository::select_all::<TransactionEntity>(&mut tx, vec![]).await?.len(), 2);
    tx.commit().await?;
    assert_eq!(count(&db).await?, 2);

    // rollback discards
    let mut tx = db.begin().await?;
    repository::insert(&mut tx, &entity(3)).await?;
    tx.rollback().await?;
    assert_eq!(count(&db).await?, 2);

    // drop without commit discards
    {
        let mut tx = db.begin().await?;
        repository::insert(&mut tx, &entity(4)).await?;
    }
    assert_eq!(count(&db).await?, 2);

    // a failed statement aborts the transaction, commit returns error and rolls back
    let mut tx = db.begin().await?;
    repository::insert(&mut tx, &entity(5)).await?;
    assert!(repository::insert(&mut tx, &entity(1)).await.is_err(), "duplicate key must fail");
    assert!(tx.commit().await.is_err(), "commit after failed statement must fail");
    assert_eq!(count(&db).await?, 2);

    // a statement cancelled by caller still runs on the connection, transaction must not commit
    let mut tx = db.begin().await?;
    repository::insert(&mut tx, &entity(7)).await?;
    let slow = database::execute(&mut tx, "SELECT pg_sleep(1)", &[]);
    assert!(tokio::time::timeout(Duration::from_millis(100), slow).await.is_err(), "slow statement must be cancelled");
    assert!(tx.commit().await.is_err(), "commit after cancelled statement must fail");
    assert_eq!(count(&db).await?, 2);

    // db is usable after a failed transaction, the broken connection was discarded
    let mut tx = db.begin().await?;
    repository::insert(&mut tx, &entity(6)).await?;
    tx.commit().await?;
    assert_eq!(count(&db).await?, 3);

    Ok(())
}
