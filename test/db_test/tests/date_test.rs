use db_test::client;
use framework::exception::Exception;
use framework::time::Date as FrameworkDate;
use framework::time::DateTime;
use framework::time::Offset;
use framework_db::Database;
use framework_db::database;
use framework_db::repository;
use framework_db::types::Date;
use framework_db::types::Timestamp;
use framework_macro::Entity;
use framework_macro::integration_test;

#[derive(Entity, Debug, PartialEq, Clone)]
#[table(name = "date_entity")]
struct DateEntity {
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
    database::execute(db, "DROP TABLE IF EXISTS \"date_entity\"", &[]).await?;
    database::execute(
        db,
        "CREATE TABLE \"date_entity\" (
            id                      BIGINT PRIMARY KEY,
            timestamp_col           TIMESTAMP(6) WITH TIME ZONE NOT NULL,
            date_col                DATE NOT NULL,
            nullable_timestamp_col  TIMESTAMP(6) WITH TIME ZONE,
            nullable_date_col       DATE
        )",
        &[],
    )
    .await?;
    Ok(())
}

#[integration_test]
async fn date_type() -> Result<(), Exception> {
    let db = client();
    setup_schema(&db).await?;

    // microseconds on purpose, timestamptz keeps them and a millis based mapping would truncate here
    let date_time = DateTime::parse("2026-07-16T08:30:45.123456Z")?.with_timezone(Offset::new(8, 0));
    let entity = DateEntity {
        id: 1,
        timestamp_col: Timestamp::from(date_time),
        date_col: Date::from(date_time.date()),
        nullable_timestamp_col: None,
        nullable_date_col: None,
    };
    repository::insert(&db, &entity).await?;

    assert_eq!(repository::select_one(&db, vec![DateEntity::ID.eq(1)]).await?, Some(entity.clone()));

    // a param binds through ToSql, so the value the server compares against is the wire form,
    // not the debug/display form of the newtype
    let selected_entity = repository::select_one(
        &db,
        vec![
            DateEntity::TIMESTAMP_COL.eq(Timestamp::from(date_time)),
            DateEntity::DATE_COL.eq(Date::from(date_time.date())),
        ],
    )
    .await?;
    assert_eq!(selected_entity, Some(entity));

    // what the server actually stored, a wrong epoch base or scale fails here even though the
    // symmetric round trip above would still pass
    let stored: Option<String> = database::select_one(
        &db,
        "SELECT to_char(timestamp_col AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US') FROM \"date_entity\" WHERE id = $1",
        &[&1_i64],
    )
    .await?;

    assert_eq!(stored, Some("2026-07-16 08:30:45.123456".to_owned()));

    let stored: Option<String> = database::select_one(
        &db,
        "SELECT to_char(date_col, 'YYYY-MM-DD') FROM \"date_entity\" WHERE id = $1",
        &[&1_i64],
    )
    .await?;
    assert_eq!(stored, Some("2026-07-16".to_owned()));

    // before the 2000-01-01 postgres epoch, both mappings go negative there
    let before_epoch = DateEntity {
        id: 2,
        timestamp_col: Timestamp::from(DateTime::parse("1970-01-01T00:00:00Z")?),
        date_col: Date::from(FrameworkDate::new(1970, 1, 1)),
        nullable_timestamp_col: Some(Timestamp::from(date_time)),
        nullable_date_col: Some(Date::from(date_time.date())),
    };
    repository::insert(&db, &before_epoch).await?;
    assert_eq!(repository::select_one(&db, vec![DateEntity::ID.eq(2)]).await?, Some(before_epoch));

    // a nullable column is set to NULL with update(None)
    let updated = repository::update(
        &db,
        vec![DateEntity::NULLABLE_TIMESTAMP_COL.update(None), DateEntity::NULLABLE_DATE_COL.update(None)],
        vec![DateEntity::ID.eq(2)],
    )
    .await?;
    assert_eq!(updated, 1);
    let entity = repository::select_one(&db, vec![DateEntity::ID.eq(2)]).await?.unwrap();
    assert_eq!(entity.nullable_timestamp_col, None);
    assert_eq!(entity.nullable_date_col, None);

    assert_eq!(repository::delete::<DateEntity>(&db, vec![]).await?, 2);

    Ok(())
}
