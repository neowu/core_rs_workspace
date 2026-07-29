use chrono::TimeDelta;
use chrono::TimeZone as _;
use chrono::Utc;
use clickhouse_test::client;
use clickhouse_test::flush;
use framework::exception::Exception;
use framework_clickhouse::ClickHouse;
use framework_clickhouse::Enum8;
use framework_clickhouse::clickhouse;
use framework_clickhouse::clickhouse::Row;
use framework_clickhouse::data_type::Date;
use framework_clickhouse::data_type::DateTime64;
use framework_clickhouse::data_type::Decimal64;
use framework_macro::integration_test;
use serde::Deserialize;
use serde::Serialize;

// Enum8('OK' = 1, 'WARN' = 2, 'ERROR' = 3)
#[derive(Enum8, Debug, PartialEq)]
enum Level {
    Ok = 1,
    Warn = 2,
    Error = 3,
}

// column is named `enum` (a rust keyword); serde strips the r# prefix, so r#enum maps to it
#[derive(Row, Serialize, Deserialize, Debug, PartialEq)]
struct DataTypeEntity {
    id: String,
    date: Date,
    time: DateTime64,
    local_time: DateTime64,
    decimal: Decimal64<6>,
    levels: Vec<Level>,
}

async fn setup_schema(clickhouse: &ClickHouse) -> Result<(), Exception> {
    clickhouse.execute("DROP TABLE IF EXISTS data_type_entity", &[]).await?;
    clickhouse
        .execute(
            "CREATE TABLE IF NOT EXISTS data_type_entity (
                    id          String,
                    date        Date,
                    time        DateTime64(3, 'UTC'),
                    local_time  DateTime64(3, 'Asia/Hong_Kong'),
                    decimal     Decimal64(6),
                    levels      Array(Enum8('OK' = 1, 'WARN' = 2, 'ERROR' = 3))
                ) ENGINE = MergeTree ORDER BY (id)",
            &[],
        )
        .await?;
    Ok(())
}

#[integration_test]
async fn data_type() -> Result<(), Exception> {
    let clickhouse = client();

    setup_schema(&clickhouse).await?;

    let time = DateTime64::from(Utc.with_ymd_and_hms(2026, 7, 16, 8, 30, 45).unwrap() + TimeDelta::milliseconds(123));
    let entities = [DataTypeEntity {
        id: "1".to_owned(),
        date: Date::from(time.date_naive()),
        time,
        local_time: time,
        decimal: Decimal64::from_f64(12.345_678),
        levels: vec![Level::Warn, Level::Error],
    }];
    clickhouse.insert("data_type_entity", &entities).await?;
    flush(&clickhouse).await?;

    let entity = clickhouse
        .select_one::<DataTypeEntity>("SELECT ?fields FROM data_type_entity WHERE id = ?", &[&"1"])
        .await?
        .unwrap();
    assert_eq!(entity, entities[0]);

    // params take the human readable branch of the newtypes, so Date binds as 'YYYY-MM-DD' and
    // DateTime64 as RFC3339; their RowBinary forms are bare numbers, which the server either
    // rejects (Date) or silently fails to match (DateTime64)
    let entity = clickhouse
        .select_one::<DataTypeEntity>(
            "SELECT ?fields FROM data_type_entity WHERE date = ? AND time = ? AND local_time = ?",
            &[&entities[0].date, &time, &time],
        )
        .await?
        .unwrap();
    assert_eq!(entity, entities[0]);

    // toString() renders what the server sees, so a wrong scale / timezone / enum mapping
    // fails here even though the symmetric serde round trip above would still pass
    let (time, date, local_time, decimal, levels) = clickhouse
        .select_one::<(String, String, String, String, String)>(
            "SELECT toString(time), toString(date), toString(local_time), toString(decimal), toString(levels) FROM data_type_entity WHERE id = ?",
            &[&"1"],
        )
        .await?.unwrap();
    assert_eq!(time, "2026-07-16 08:30:45.123");
    assert_eq!(date, "2026-07-16");
    // same instant rendered in the column timezone, Asia/Hong_Kong (UTC+8)
    assert_eq!(local_time, "2026-07-16 16:30:45.123");
    assert_eq!(decimal, "12.345678");
    assert_eq!(levels, "['WARN','ERROR']");

    Ok(())
}
