use chrono::TimeDelta;
use chrono::TimeZone as _;
use chrono::Utc;
use clickhouse_test::client;
use clickhouse_test::run_test;
use clickhouse_test::select_one_with_retry;
use framework::exception::Exception;
use framework_clickhouse::Enum8;
use framework_clickhouse::Identifier;
use framework_clickhouse::clickhouse;
use framework_clickhouse::clickhouse::Row;
use framework_clickhouse::data_type::DateTime64;
use framework_clickhouse::data_type::Decimal64;
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
    time: DateTime64,
    local_time: DateTime64,
    decimal: Decimal64<6>,
    levels: Vec<Level>,
}

#[tokio::test]
async fn data_type() {
    run_test("data_type", async {
        let db = setup().await?;

        let clickhouse = client(Some(db));


        let time = DateTime64::from(Utc.with_ymd_and_hms(2026, 7, 16, 8, 30, 45).unwrap() + TimeDelta::milliseconds(123));
        let entities = [
            DataTypeEntity {
                id: "1".to_string(),
                time,
                local_time: time,
                decimal: Decimal64::from_f64(12.345_678),
                levels: vec![Level::Warn, Level::Error],
            }
        ];
        clickhouse.insert("data_type_entity", &entities).await?;

        assert_eq!(select_one_with_retry::<DataTypeEntity>(&clickhouse, "SELECT ?fields FROM data_type_entity WHERE id = ?", &[&"1"]).await?, entities[0]);

        // toString() renders what the server sees, so a wrong scale / timezone / enum mapping
        // fails here even though the symmetric serde round trip above would still pass
        let (time, local_time, decimal, levels): (String, String, String, String) = clickhouse
            .select_one(
                "SELECT toString(time), toString(local_time), toString(decimal), toString(levels) FROM data_type_entity WHERE id = ?",
                &[&"1"],
            )
            .await?;
        assert_eq!(time, "2026-07-16 08:30:45.123");
        // same instant rendered in the column timezone, Asia/Hong_Kong (UTC+8)
        assert_eq!(local_time, "2026-07-16 16:30:45.123");
        assert_eq!(decimal, "12.345678");
        assert_eq!(levels, "['WARN','ERROR']");

        Ok(())
    })
    .await;
}

async fn setup() -> Result<&'static str, Exception> {
    let admin = client(None);
    let db = Identifier("clickhouse_test");

    admin.execute("DROP DATABASE IF EXISTS ?", &[&db]).await?;
    admin.execute("CREATE DATABASE IF NOT EXISTS ?", &[&db]).await?;
    admin
        .execute(
            "CREATE TABLE IF NOT EXISTS ?.data_type_entity (
                    id          String,
                    time        DateTime64(3, 'UTC'),
                    local_time  DateTime64(3, 'Asia/Hong_Kong'),
                    decimal     Decimal64(6),
                    levels      Array(Enum8('OK' = 1, 'WARN' = 2, 'ERROR' = 3))
                ) ENGINE = MergeTree ORDER BY (id)",
            &[&db],
        )
        .await?;
    Ok(db.0)
}
