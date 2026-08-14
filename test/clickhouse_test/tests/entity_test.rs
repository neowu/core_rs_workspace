use clickhouse_test::client;
use clickhouse_test::flush;
use framework::exception::Exception;
use framework_clickhouse::ClickHouse;
use framework_clickhouse::Enum8;
use framework_clickhouse::clickhouse;
use framework_clickhouse::clickhouse::Row;
use framework_macro::integration_test;
use serde::Deserialize;
use serde::Serialize;

#[derive(Enum8, Debug, Clone, PartialEq)]
enum Level {
    Ok = 1,
    Warn = 2,
    Error = 3,
}

#[derive(Row, Serialize, Deserialize, Debug, Clone, PartialEq)]
struct Entity {
    id: String,
    count: i32,
    tags: Vec<String>,
    levels: Vec<Level>,
}

async fn setup_schema(clickhouse: &ClickHouse) -> Result<(), Exception> {
    clickhouse.execute("DROP TABLE IF EXISTS entity", &[]).await?;
    clickhouse
        .execute(
            "CREATE TABLE IF NOT EXISTS entity (
                    id          String,
                    count       Int32,
                    tags        Array(String),
                    levels      Array(Enum8('OK' = 1, 'WARN' = 2, 'ERROR' = 3))
                ) ENGINE = MergeTree ORDER BY (id)",
            &[],
        )
        .await?;
    Ok(())
}

#[integration_test]
async fn entity() -> Result<(), Exception> {
    let clickhouse = client();

    setup_schema(&clickhouse).await?;

    let entities = [
        Entity {
            id: "1".to_owned(),
            count: 42,
            tags: vec!["alpha".to_owned(), "beta".to_owned()],
            levels: vec![Level::Warn, Level::Error],
        },
        // the bounds of Int32 and the empty array both round trip
        Entity { id: "2".to_owned(), count: i32::MIN, tags: vec![], levels: vec![Level::Ok] },
        Entity { id: "3".to_owned(), count: i32::MAX, tags: vec!["".to_owned()], levels: vec![] },
    ];
    clickhouse.insert("entity", &entities).await?;
    flush(&clickhouse).await?;

    for expected in &entities {
        let entity =
            clickhouse.select_one::<Entity>("SELECT ?fields FROM entity WHERE id = ?", &[&expected.id]).await?.unwrap();
        assert_eq!(entity, *expected);
    }

    // toString() renders what the server sees, so a wrong enum mapping or array element type
    // fails here even though the symmetric serde round trip above would still pass
    let (count, tags, levels) = clickhouse
        .select_one::<(String, String, String)>(
            "SELECT toString(count), toString(tags), toString(levels) FROM entity WHERE id = ?",
            &[&"1"],
        )
        .await?
        .unwrap();
    assert_eq!(count, "42");
    assert_eq!(tags, "['alpha','beta']");
    assert_eq!(levels, "['WARN','ERROR']");

    let (count, tags, levels) = clickhouse
        .select_one::<(String, String, String)>(
            "SELECT toString(count), toString(tags), toString(levels) FROM entity WHERE id = ?",
            &[&"2"],
        )
        .await?
        .unwrap();
    assert_eq!(count, "-2147483648");
    assert_eq!(tags, "[]");
    assert_eq!(levels, "['OK']");

    // params bind by value, so an Int32 column and an Array(String) element both match on the rust side
    let entity = clickhouse
        .select_one::<Entity>("SELECT ?fields FROM entity WHERE count = ? AND has(tags, ?)", &[&42_i32, &"alpha"])
        .await?
        .unwrap();
    assert_eq!(entity, entities[0]);

    Ok(())
}
