use clickhouse_test::client;
use clickhouse_test::flush;
use framework::exception::Exception;
use framework::time::Date;
use framework::time::DateTime;
use framework_clickhouse::ClickHouse;
use framework_clickhouse::clickhouse;
use framework_clickhouse::clickhouse::Row;
use framework_clickhouse::types::Date16;
use framework_clickhouse::types::DateTime64;
use framework_macro::integration_test;
use serde::Deserialize;
use serde::Serialize;

#[derive(Row, Serialize, Deserialize, Debug, Clone, PartialEq)]
struct DateEntity {
    id: String,
    date: Date16,
    time: DateTime64,
    local_time: DateTime64,
}

async fn setup_schema(clickhouse: &ClickHouse) -> Result<(), Exception> {
    clickhouse.execute("DROP TABLE IF EXISTS date_entity", &[]).await?;
    clickhouse
        .execute(
            "CREATE TABLE IF NOT EXISTS date_entity (
                    id          String,
                    date        Date,
                    time        DateTime64(3, 'UTC'),
                    local_time  DateTime64(3, 'Asia/Hong_Kong')
                ) ENGINE = MergeTree ORDER BY (id)",
            &[],
        )
        .await?;
    Ok(())
}

#[integration_test]
async fn date() -> Result<(), Exception> {
    let clickhouse = client();

    setup_schema(&clickhouse).await?;

    let date_time = DateTime::parse("2026-07-16T08:30:45.123Z")?;
    let time = DateTime64::from(date_time);
    let entities = [
        DateEntity { id: "1".to_owned(), date: Date16::from(date_time.date()), time, local_time: time },
        // 2149-06-06 is the last day the u16 days of Date can address
        DateEntity { id: "2".to_owned(), date: Date16::from(Date::new(2149, 6, 6)), time, local_time: time },
    ];
    clickhouse.insert("date_entity", &entities).await?;
    flush(&clickhouse).await?;

    let entity =
        clickhouse.select_one::<DateEntity>("SELECT ?fields FROM date_entity WHERE id = ?", &[&"1"]).await?.unwrap();
    assert_eq!(entity, entities[0]);

    // params take the human readable branch of the newtypes, so Date16 binds as 'YYYY-MM-DD' and
    // DateTime64 as RFC3339; their RowBinary forms are bare numbers, which the server either
    // rejects (Date16) or silently fails to match (DateTime64)
    let entity = clickhouse
        .select_one::<DateEntity>(
            "SELECT ?fields FROM date_entity WHERE date = ? AND time = ? AND local_time = ?",
            &[&entities[0].date, &time, &time],
        )
        .await?
        .unwrap();
    assert_eq!(entity, entities[0]);

    // toString() renders what the server sees, so a wrong scale / timezone
    // fails here even though the symmetric serde round trip above would still pass
    let (time, date, local_time) = clickhouse
        .select_one::<(String, String, String)>(
            "SELECT toString(time), toString(date), toString(local_time) FROM date_entity WHERE id = ?",
            &[&"1"],
        )
        .await?
        .unwrap();
    assert_eq!(time, "2026-07-16 08:30:45.123");
    assert_eq!(date, "2026-07-16");
    // same instant rendered in the column timezone, Asia/Hong_Kong (UTC+8)
    assert_eq!(local_time, "2026-07-16 16:30:45.123");

    // the upper bound row round trips through the u16 days of Date
    let entity =
        clickhouse.select_one::<DateEntity>("SELECT ?fields FROM date_entity WHERE id = ?", &[&"2"]).await?.unwrap();
    assert_eq!(entity, entities[1]);

    let date =
        clickhouse.select_one::<String>("SELECT toString(date) FROM date_entity WHERE id = ?", &[&"2"]).await?.unwrap();
    assert_eq!(date, "2149-06-06");

    // the server clamps a date outside the Date range instead of failing, so the row serializer rejects it first
    let out_of_range = DateEntity { id: "3".to_owned(), date: Date16::from(Date::new(2149, 6, 7)), ..entities[1] };
    let error = clickhouse.insert("date_entity", &[out_of_range]).await.unwrap_err();
    assert!(error.to_string().contains("date is out of range, date=2149-06-07"));

    let before_epoch = DateEntity { id: "4".to_owned(), date: Date16::from(Date::new(1969, 12, 31)), ..entities[1] };
    let error = clickhouse.insert("date_entity", &[before_epoch]).await.unwrap_err();
    assert!(error.to_string().contains("date is out of range, date=1969-12-31"));

    Ok(())
}
