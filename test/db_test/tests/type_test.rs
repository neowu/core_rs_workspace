use db_test::client;
use framework::date::Date as FrameworkDate;
use framework::date::DateTime;
use framework::exception::Exception;
use framework_db::Database;
use framework_db::database;
use framework_db::repository;
use framework_db::types::Date;
use framework_db::types::Timestamp;
use framework_macro::Entity;
use framework_macro::integration_test;

#[derive(Entity, Debug, PartialEq)]
#[table(name = "data_type_entity")]
struct DataTypeEntity {
    #[primary_key]
    #[column(name = "id")]
    id: i64,
    #[column(name = "timestamp_col")]
    timestamp_col: Timestamp,
    #[column(name = "date_col")]
    date_col: Date,
    #[column(name = "nullable_timestamp_col")]
    nullable_timestamp_col: Option<Timestamp>,
    #[column(name = "nullable_date_col")]
    nullable_date_col: Option<Date>,
}

async fn setup_schema(db: &Database) -> Result<(), Exception> {
    database::execute(db, "DROP TABLE IF EXISTS \"data_type_entity\"", &[]).await?;
    database::execute(
        db,
        "CREATE TABLE \"data_type_entity\" (
            id                      BIGINT PRIMARY KEY,
            timestamp_col           TIMESTAMPTZ NOT NULL,
            date_col                DATE NOT NULL,
            nullable_timestamp_col  TIMESTAMPTZ,
            nullable_date_col       DATE
        )",
        &[],
    )
    .await?;
    Ok(())
}

#[integration_test]
async fn data_type() -> Result<(), Exception> {
    let db = client()?;
    setup_schema(&db).await?;

    // microseconds on purpose, timestamptz keeps them and a millis based mapping would truncate here
    let date_time = DateTime::parse("2026-07-16T08:30:45.123456Z")?;
    let entity = DataTypeEntity {
        id: 1,
        timestamp_col: Timestamp::from(date_time),
        date_col: Date::from(date_time.date()),
        nullable_timestamp_col: None,
        nullable_date_col: None,
    };
    repository::insert(&db, &entity).await?;

    assert_eq!(repository::select_one(&db, vec![DataTypeEntity::ID.eq(1)]).await?, Some(entity));

    // a param binds through ToSql, so the value the server compares against is the wire form,
    // not the debug/display form of the newtype
    let entity = repository::select_one(
        &db,
        vec![
            DataTypeEntity::TIMESTAMP_COL.eq(Timestamp::from(date_time)),
            DataTypeEntity::DATE_COL.eq(Date::from(date_time.date())),
        ],
    )
    .await?;
    assert_eq!(entity.map(|e| e.id), Some(1));

    // what the server actually stored, a wrong epoch base or scale fails here even though the
    // symmetric round trip above would still pass
    let stored: Option<String> = database::select_one(
        &db,
        "SELECT to_char(timestamp_col AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US') FROM \"data_type_entity\" WHERE id = $1",
        &[&1_i64],
    )
    .await?;
    assert_eq!(stored, Some("2026-07-16 08:30:45.123456".to_owned()));

    let stored: Option<String> = database::select_one(
        &db,
        "SELECT to_char(date_col, 'YYYY-MM-DD') FROM \"data_type_entity\" WHERE id = $1",
        &[&1_i64],
    )
    .await?;
    assert_eq!(stored, Some("2026-07-16".to_owned()));

    // before the 2000-01-01 postgres epoch, both mappings go negative there
    let before_epoch = DataTypeEntity {
        id: 2,
        timestamp_col: Timestamp::from(DateTime::parse("1970-01-01T00:00:00Z")?),
        date_col: Date::from(FrameworkDate::new(1970, 1, 1)),
        nullable_timestamp_col: Some(Timestamp::from(date_time)),
        nullable_date_col: Some(Date::from(date_time.date())),
    };
    repository::insert(&db, &before_epoch).await?;
    assert_eq!(repository::select_one(&db, vec![DataTypeEntity::ID.eq(2)]).await?, Some(before_epoch));

    // a nullable column is set to NULL with update(None)
    let updated = repository::update(
        &db,
        vec![DataTypeEntity::NULLABLE_TIMESTAMP_COL.update(None), DataTypeEntity::NULLABLE_DATE_COL.update(None)],
        vec![DataTypeEntity::ID.eq(2)],
    )
    .await?;
    assert_eq!(updated, 1);
    let entity = repository::select_one(&db, vec![DataTypeEntity::ID.eq(2)]).await?.unwrap();
    assert_eq!(entity.nullable_timestamp_col, None);
    assert_eq!(entity.nullable_date_col, None);

    // infinity does not fit either framework type, decoding fails instead of returning a bogus value
    let sql = "UPDATE \"data_type_entity\" SET timestamp_col = 'infinity' WHERE id = $1";
    database::execute(&db, sql, &[&2_i64]).await?;
    let error = repository::select_one::<DataTypeEntity>(&db, vec![DataTypeEntity::ID.eq(2)]).await.unwrap_err();
    assert!(error.to_string().contains("timestamp is out of range"));

    assert_eq!(repository::delete::<DataTypeEntity>(&db, vec![]).await?, 2);

    Ok(())
}
