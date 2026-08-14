use clickhouse_test::client;
use clickhouse_test::flush;
use framework::exception::Exception;
use framework_clickhouse::ClickHouse;
use framework_clickhouse::clickhouse;
use framework_clickhouse::clickhouse::Row;
use framework_clickhouse::types::Decimal64;
use framework_macro::integration_test;
use serde::Deserialize;
use serde::Serialize;

#[derive(Row, Serialize, Deserialize, Debug, Clone, PartialEq)]
struct DecimalEntity {
    id: String,
    decimal: Decimal64<6>,
}

async fn setup_schema(clickhouse: &ClickHouse) -> Result<(), Exception> {
    clickhouse.execute("DROP TABLE IF EXISTS decimal_entity", &[]).await?;
    clickhouse
        .execute(
            "CREATE TABLE IF NOT EXISTS decimal_entity (
                    id          String,
                    decimal     Decimal64(6)
                ) ENGINE = MergeTree ORDER BY (id)",
            &[],
        )
        .await?;
    Ok(())
}

#[integration_test]
async fn decimal() -> Result<(), Exception> {
    let clickhouse = client();

    setup_schema(&clickhouse).await?;

    let entities = [
        DecimalEntity { id: "1".to_owned(), decimal: Decimal64::from_f64(12.345_678) },
        // the smallest magnitude the scale can carry, on the negative side
        DecimalEntity { id: "2".to_owned(), decimal: Decimal64::from_f64(-0.000_001) },
    ];
    clickhouse.insert("decimal_entity", &entities).await?;
    flush(&clickhouse).await?;

    let entity = clickhouse
        .select_one::<DecimalEntity>("SELECT ?fields FROM decimal_entity WHERE id = ?", &[&"1"])
        .await?
        .unwrap();
    assert_eq!(entity, entities[0]);

    // toString() renders what the server sees, so a wrong scale fails here
    // even though the symmetric serde round trip above would still pass
    let decimal = clickhouse
        .select_one::<String>("SELECT toString(decimal) FROM decimal_entity WHERE id = ?", &[&"1"])
        .await?
        .unwrap();
    assert_eq!(decimal, "12.345678");

    let entity = clickhouse
        .select_one::<DecimalEntity>("SELECT ?fields FROM decimal_entity WHERE id = ?", &[&"2"])
        .await?
        .unwrap();
    assert_eq!(entity, entities[1]);

    let decimal = clickhouse
        .select_one::<String>("SELECT toString(decimal) FROM decimal_entity WHERE id = ?", &[&"2"])
        .await?
        .unwrap();
    assert_eq!(decimal, "-0.000001");

    Ok(())
}
