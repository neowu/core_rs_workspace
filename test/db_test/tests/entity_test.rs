use db_test::client;
use db_test::run_test;
use framework::exception::Exception;
use framework_db::Database;
use framework_db::database;
use framework_db::repository;
use framework_macro::Entity;
use uuid::Uuid;

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

// each test manages only its own table so the two tests stay independent under
// cargo's parallel test execution
async fn create_auto_increment_table(db: &Database) -> Result<(), Exception> {
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

async fn create_composite_table(db: &Database) -> Result<(), Exception> {
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

#[tokio::test]
async fn auto_increment_id_entity() {
    run_test("auto_increment_id_entity", async {
        let db = client()?;
        create_auto_increment_table(&db).await?;

        // insert returns the generated id
        let id = repository::insert_with_auto_increment_id(
            &db,
            &AutoIncrementIdEntity { id: None, text_col: Some("hello".to_owned()), decimal_col: 1.5 },
        )
        .await?;
        assert!(id > 0);

        // get by primary key
        let entity = repository::select_one(&db, vec![AutoIncrementIdEntity::FIELD_ID.eq(id)]).await?;
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
        let rows = repository::select_all(&db, vec![AutoIncrementIdEntity::FIELD_TEXT_COL.not_null()]).await?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, Some(id));

        let rows =
            repository::select_all(&db, vec![AutoIncrementIdEntity::FIELD_TEXT_COL.eq(Some("hello".to_owned()))])
                .await?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, Some(id));

        // update
        let updated = repository::update(
            &db,
            vec![AutoIncrementIdEntity::FIELD_TEXT_COL.update(Some("world".to_owned()))],
            vec![AutoIncrementIdEntity::FIELD_ID.eq(id)],
        )
        .await?;
        assert_eq!(updated, 1);

        let entity = repository::select_one(&db, vec![AutoIncrementIdEntity::FIELD_ID.eq(id)]).await?;
        assert_eq!(entity.and_then(|e| e.text_col), Some("world".to_owned()));

        // a nullable column is set to NULL with update(None)
        let updated = repository::update(
            &db,
            vec![AutoIncrementIdEntity::FIELD_TEXT_COL.update(None)],
            vec![AutoIncrementIdEntity::FIELD_ID.eq(id)],
        )
        .await?;
        assert_eq!(updated, 1);

        let entity = repository::select_one(&db, vec![AutoIncrementIdEntity::FIELD_ID.eq(id)]).await?;
        assert_eq!(entity.and_then(|e| e.text_col), None);

        // delete
        assert_eq!(repository::delete(&db, vec![AutoIncrementIdEntity::FIELD_ID.eq(id)]).await?, 1);
        assert_eq!(repository::delete(&db, vec![AutoIncrementIdEntity::FIELD_ID.eq(id2)]).await?, 1);
        assert_eq!(repository::select_one(&db, vec![AutoIncrementIdEntity::FIELD_ID.eq(id)]).await?, None);

        Ok(())
    })
    .await;
}

#[tokio::test]
async fn composite_id_entity() {
    run_test("composite_id_entity", async {
        let db = client()?;
        create_composite_table(&db).await?;

        let uuid = Uuid::now_v7();
        let entity = CompositeIdEntity { id1: 1, id2: "id".to_owned(), uuid_col: uuid, bool_col: true };

        // insert
        repository::insert(&db, &entity).await?;

        // select by composite primary key
        assert_eq!(
            repository::select_one(
                &db,
                vec![CompositeIdEntity::FIELD_ID1.eq(1), CompositeIdEntity::FIELD_ID2.eq("id".to_owned())]
            )
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
        let inserted = repository::upsert(
            &db,
            &CompositeIdEntity { id1: 1, id2: "id".to_owned(), uuid_col: uuid, bool_col: false },
        )
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
            vec![CompositeIdEntity::FIELD_BOOL_COL.update(true)],
            vec![CompositeIdEntity::FIELD_ID1.eq(1), CompositeIdEntity::FIELD_ID2.eq("id".to_owned())],
        )
        .await?;
        assert_eq!(updated, 1);
        assert_eq!(
            repository::select_one(
                &db,
                vec![CompositeIdEntity::FIELD_ID1.eq(1), CompositeIdEntity::FIELD_ID2.eq("id".to_owned())]
            )
            .await?
            .map(|e| e.bool_col),
            Some(true)
        );

        // select_all
        let rows = repository::select_all::<CompositeIdEntity>(&db, vec![]).await?;
        assert_eq!(rows.len(), 2);

        // delete
        assert_eq!(
            repository::delete(
                &db,
                vec![CompositeIdEntity::FIELD_ID1.eq(1), CompositeIdEntity::FIELD_ID2.eq("id".to_owned())]
            )
            .await?,
            1
        );
        assert_eq!(
            repository::select_one(
                &db,
                vec![CompositeIdEntity::FIELD_ID1.eq(1), CompositeIdEntity::FIELD_ID2.eq("id".to_owned())]
            )
            .await?,
            None
        );

        // delete remaining
        let deleted = repository::delete::<CompositeIdEntity>(&db, vec![]).await?;
        assert_eq!(deleted, 1);

        Ok(())
    })
    .await;
}
