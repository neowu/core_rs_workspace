use db_test::client;
use framework::exception::Exception;
use framework_db::Database;
use framework_db::database;
use framework_db::repository;
use framework_macro::Entity;
use framework_macro::integration_test;

#[derive(Entity, Debug, PartialEq)]
#[table(name = "auto_increment_id_entity")]
struct AutoIncrementIdEntity {
    #[primary_key(auto_increment)]
    #[column(name = "id")]
    id: Option<i64>,
    #[column(name = "text_col")]
    text_col: Option<String>,
    #[column(name = "decimal_col")]
    decimal_col: f64,
}

async fn setup_schema(db: &Database) -> Result<(), Exception> {
    database::execute(db, "DROP TABLE IF EXISTS \"auto_increment_id_entity\"", &[]).await?;
    database::execute(
        db,
        "CREATE TABLE \"auto_increment_id_entity\" (
            id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            text_col     TEXT,
            decimal_col  DOUBLE PRECISION NOT NULL
        )",
        &[],
    )
    .await?;
    Ok(())
}

#[integration_test]
async fn auto_increment_id_entity() -> Result<(), Exception> {
    let db = client()?;
    setup_schema(&db).await?;

    // insert returns the generated id
    let id = repository::insert_with_auto_increment_id(
        &db,
        &AutoIncrementIdEntity { id: None, text_col: Some("hello".to_owned()), decimal_col: 1.5 },
    )
    .await?;
    assert!(id > 0);

    // get by primary key
    let entity = repository::select_one(&db, vec![AutoIncrementIdEntity::ID.eq(id)]).await?;
    assert_eq!(
        entity,
        Some(AutoIncrementIdEntity { id: Some(id), text_col: Some("hello".to_owned()), decimal_col: 1.5 })
    );

    // second row, so select_all has something to filter
    let id2 = repository::insert_with_auto_increment_id(
        &db,
        &AutoIncrementIdEntity { id: None, text_col: None, decimal_col: 2.5 },
    )
    .await?;

    // select_all with a condition
    let rows = repository::select_all(&db, vec![AutoIncrementIdEntity::TEXT_COL.not_null()]).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, Some(id));

    let rows = repository::select_all(&db, vec![AutoIncrementIdEntity::TEXT_COL.eq(Some("hello".to_owned()))]).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, Some(id));

    // update
    let updated = repository::update(
        &db,
        vec![AutoIncrementIdEntity::TEXT_COL.update(Some("world".to_owned()))],
        vec![AutoIncrementIdEntity::ID.eq(id)],
    )
    .await?;
    assert_eq!(updated, 1);

    let entity = repository::select_one(&db, vec![AutoIncrementIdEntity::ID.eq(id)]).await?;
    assert_eq!(entity.and_then(|e| e.text_col), Some("world".to_owned()));

    // a nullable column is set to NULL with update(None)
    let updated = repository::update(
        &db,
        vec![AutoIncrementIdEntity::TEXT_COL.update(None)],
        vec![AutoIncrementIdEntity::ID.eq(id)],
    )
    .await?;
    assert_eq!(updated, 1);

    let entity = repository::select_one(&db, vec![AutoIncrementIdEntity::ID.eq(id)]).await?;
    assert_eq!(entity.and_then(|e| e.text_col), None);

    // delete
    assert_eq!(repository::delete(&db, vec![AutoIncrementIdEntity::ID.eq(id)]).await?, 1);
    assert_eq!(repository::delete(&db, vec![AutoIncrementIdEntity::ID.eq(id2)]).await?, 1);
    assert_eq!(repository::select_one(&db, vec![AutoIncrementIdEntity::ID.eq(id)]).await?, None);

    Ok(())
}
