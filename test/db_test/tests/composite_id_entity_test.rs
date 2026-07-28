use db_test::client;
use framework::exception::Exception;
use framework_db::Database;
use framework_db::database;
use framework_db::repository;
use framework_macro::Entity;
use framework_macro::integration_test;
use uuid::Uuid;

#[derive(Entity, Debug, PartialEq)]
#[table(name = "composite_id_entity")]
struct CompositeIdEntity {
    #[primary_key]
    #[column(name = "id1")]
    id1: i64,
    #[primary_key]
    #[column(name = "id2")]
    id2: String,
    #[column(name = "uuid_col")]
    uuid_col: Uuid,
    #[column(name = "bool_col")]
    bool_col: bool,
}

async fn setup_schema(db: &Database) -> Result<(), Exception> {
    database::execute(db, "DROP TABLE IF EXISTS \"composite_id_entity\"", &[]).await?;
    database::execute(
        db,
        "CREATE TABLE \"composite_id_entity\" (
            id1       BIGINT NOT NULL,
            id2       TEXT NOT NULL,
            uuid_col  UUID NOT NULL,
            bool_col  BOOLEAN NOT NULL,
            PRIMARY KEY (id1, id2)
        )",
        &[],
    )
    .await?;
    Ok(())
}

#[integration_test]
async fn composite_id_entity() -> Result<(), Exception> {
    let db = client()?;
    setup_schema(&db).await?;

    let uuid = Uuid::now_v7();
    let entity = CompositeIdEntity { id1: 1, id2: "id".to_owned(), uuid_col: uuid, bool_col: true };

    // insert
    repository::insert(&db, &entity).await?;

    // select by composite primary key
    assert_eq!(
        repository::select_one(&db, vec![CompositeIdEntity::ID1.eq(1), CompositeIdEntity::ID2.eq("id".to_owned())])
            .await?,
        Some(entity)
    );

    // insert_ignore on conflict returns false (already present)
    let inserted = repository::insert_ignore(
        &db,
        &CompositeIdEntity { id1: 1, id2: "id".to_owned(), uuid_col: uuid, bool_col: false },
    )
    .await?;
    assert!(!inserted);

    // upsert existing row -> update, returns false (not inserted)
    let inserted =
        repository::upsert(&db, &CompositeIdEntity { id1: 1, id2: "id".to_owned(), uuid_col: uuid, bool_col: false })
            .await?;
    assert!(!inserted);

    // upsert new row -> insert, returns true
    let inserted = repository::upsert(
        &db,
        &CompositeIdEntity { id1: 2, id2: "b".to_owned(), uuid_col: Uuid::now_v7(), bool_col: true },
    )
    .await?;
    assert!(inserted);

    // update
    let updated = repository::update(
        &db,
        vec![CompositeIdEntity::BOOL_COL.update(true)],
        vec![CompositeIdEntity::ID1.eq(1), CompositeIdEntity::ID2.eq("id".to_owned())],
    )
    .await?;
    assert_eq!(updated, 1);
    assert_eq!(
        repository::select_one(&db, vec![CompositeIdEntity::ID1.eq(1), CompositeIdEntity::ID2.eq("id".to_owned())])
            .await?
            .map(|e| e.bool_col),
        Some(true)
    );

    // select_all
    let rows = repository::select_all::<CompositeIdEntity>(&db, vec![]).await?;
    assert_eq!(rows.len(), 2);

    // delete
    assert_eq!(
        repository::delete(&db, vec![CompositeIdEntity::ID1.eq(1), CompositeIdEntity::ID2.eq("id".to_owned())]).await?,
        1
    );
    assert_eq!(
        repository::select_one(&db, vec![CompositeIdEntity::ID1.eq(1), CompositeIdEntity::ID2.eq("id".to_owned())])
            .await?,
        None
    );

    // delete remaining
    let deleted = repository::delete::<CompositeIdEntity>(&db, vec![]).await?;
    assert_eq!(deleted, 1);

    Ok(())
}
