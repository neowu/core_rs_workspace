use clickhouse_test::client;
use clickhouse_test::flush;
use framework::time::Date;
use framework::time::DateTime;
use framework::exception::Exception;
use framework_clickhouse::ClickHouse;
use framework_clickhouse::Enum8;
use framework_clickhouse::clickhouse;
use framework_clickhouse::clickhouse::Row;
use framework_clickhouse::types::Date16;
use framework_clickhouse::types::DateTime64;
use framework_clickhouse::types::Decimal64;
use framework_macro::integration_test;
use serde::Deserialize;
use serde::Serialize;

// Enum8('OK' = 1, 'WARN' = 2, 'ERROR' = 3)
#[derive(Enum8, Debug, Clone, PartialEq)]
enum Level {
    Ok = 1,
    Warn = 2,
    Error = 3,
}

// column is named `enum` (a rust keyword); serde strips the r# prefix, so r#enum maps to it
#[derive(Row, Serialize, Deserialize, Debug, Clone, PartialEq)]
struct DataTypeEntity {
    id: String,
    date: Date16,
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

    let date_time = DateTime::parse("2026-07-16T08:30:45.123Z")?;
    let time = DateTime64::from(date_time);
    let entities = [
        DataTypeEntity {
            id: "1".to_owned(),
            date: Date16::from(date_time.date()),
            time,
            local_time: time,
            decimal: Decimal64::from_f64(12.345_678),
            levels: vec![Level::Warn, Level::Error],
        },
        // 2149-06-06 is the last day the u16 days of Date can address
        DataTypeEntity {
            id: "2".to_owned(),
            date: Date16::from(Date::new(2149, 6, 6)),
            time,
            local_time: time,
            decimal: Decimal64::from_f64(-0.000_001),
            levels: vec![Level::Ok],
        },
    ];
    clickhouse.insert("data_type_entity", &entities).await?;
    flush(&clickhouse).await?;

    let entity = clickhouse
        .select_one::<DataTypeEntity>("SELECT ?fields FROM data_type_entity WHERE id = ?", &[&"1"])
        .await?
        .unwrap();
    assert_eq!(entity, entities[0]);

    // params take the human readable branch of the newtypes, so Date16 binds as 'YYYY-MM-DD' and
    // DateTime64 as RFC3339; their RowBinary forms are bare numbers, which the server either
    // rejects (Date16) or silently fails to match (DateTime64)
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

    // the upper bound row round trips through the u16 days of Date
    let entity = clickhouse
        .select_one::<DataTypeEntity>("SELECT ?fields FROM data_type_entity WHERE id = ?", &[&"2"])
        .await?
        .unwrap();
    assert_eq!(entity, entities[1]);

    let (date, decimal) = clickhouse
        .select_one::<(String, String)>(
            "SELECT toString(date), toString(decimal) FROM data_type_entity WHERE id = ?",
            &[&"2"],
        )
        .await?
        .unwrap();
    assert_eq!(date, "2149-06-06");
    assert_eq!(decimal, "-0.000001");

    // the server clamps a date outside the Date range instead of failing, so the row serializer rejects it first
    let out_of_range =
        DataTypeEntity { id: "3".to_owned(), date: Date16::from(Date::new(2149, 6, 7)), ..entities[1].clone() };
    let error = clickhouse.insert("data_type_entity", &[out_of_range]).await.unwrap_err();
    assert!(error.to_string().contains("date is out of Date16 range, date=2149-06-07"));

    let before_epoch =
        DataTypeEntity { id: "4".to_owned(), date: Date16::from(Date::new(1969, 12, 31)), ..entities[1].clone() };
    let error = clickhouse.insert("data_type_entity", &[before_epoch]).await.unwrap_err();
    assert!(error.to_string().contains("date is out of Date16 range, date=1969-12-31"));

    Ok(())
}
