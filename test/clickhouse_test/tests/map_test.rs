use std::collections::HashMap;

use clickhouse_test::client;
use clickhouse_test::flush;
use framework::exception::Exception;
use framework_clickhouse::ClickHouse;
use framework_clickhouse::clickhouse;
use framework_clickhouse::clickhouse::Row;
use framework_clickhouse::types::Map;
use framework_macro::integration_test;
use serde::Deserialize;
use serde::Serialize;

// the row written borrows its keys and values from somewhere else and serializes the Map columns
// straight from ordered slices; the row read back owns them, which is what an app querying the
// table does
#[derive(Row, Serialize)]
struct MapEntity<'a> {
    id: &'a str,
    context: Map<'a, String, String>,
    multi_context: Map<'a, String, Vec<String>>,
    stats: Map<'a, String, u64>,
}

#[derive(Row, Deserialize, Debug, PartialEq)]
struct StoredMapEntity {
    id: String,
    context: HashMap<String, String>,
    multi_context: HashMap<String, Vec<String>>,
    stats: HashMap<String, u64>,
}

async fn setup_schema(clickhouse: &ClickHouse) -> Result<(), Exception> {
    clickhouse.execute("DROP TABLE IF EXISTS map_entity", &[]).await?;
    clickhouse
        .execute(
            "CREATE TABLE IF NOT EXISTS map_entity (
                    id              String,
                    context         Map(String, String),
                    multi_context   Map(String, Array(String)),
                    stats           Map(String, UInt64)
                ) ENGINE = MergeTree ORDER BY (id)",
            &[],
        )
        .await?;
    Ok(())
}

#[integration_test]
async fn map() -> Result<(), Exception> {
    let clickhouse = client();

    setup_schema(&clickhouse).await?;

    let context = [("uri".to_owned(), "/health".to_owned()), ("method".to_owned(), "GET".to_owned())];
    let multi_context = [("client".to_owned(), vec!["a".to_owned(), "b".to_owned()]), ("empty".to_owned(), vec![])];
    let stats = [("elapsed".to_owned(), 42_u64), ("db_count".to_owned(), 1)];
    let empty: [(String, String); 0] = [];

    let entities = [
        MapEntity { id: "1", context: Map(&context), multi_context: Map(&multi_context), stats: Map(&stats) },
        // every column is a Map, none of them nullable, so an entity with nothing to say writes
        // three empty maps rather than nulls
        MapEntity { id: "2", context: Map(&empty), multi_context: Map(&[]), stats: Map(&[]) },
    ];
    clickhouse.insert_borrowed::<MapEntity>("map_entity", &entities).await?;
    flush(&clickhouse).await?;

    let entity = clickhouse
        .select_one::<StoredMapEntity>("SELECT ?fields FROM map_entity WHERE id = ?", &[&"1"])
        .await?
        .unwrap();
    assert_eq!(entity.context.get("uri").map(String::as_str), Some("/health"));
    assert_eq!(entity.context.get("method").map(String::as_str), Some("GET"));
    assert_eq!(entity.multi_context.get("client"), Some(&vec!["a".to_owned(), "b".to_owned()]));
    assert_eq!(entity.multi_context.get("empty"), Some(&vec![]));
    assert_eq!(entity.stats.get("elapsed"), Some(&42));
    assert_eq!(entity.stats.get("db_count"), Some(&1));

    let entity = clickhouse
        .select_one::<StoredMapEntity>("SELECT ?fields FROM map_entity WHERE id = ?", &[&"2"])
        .await?
        .unwrap();
    assert!(entity.context.is_empty());
    assert!(entity.multi_context.is_empty());
    assert!(entity.stats.is_empty());

    // a slice of pairs can repeat a key where a HashMap cannot; clickhouse stores a Map as an array
    // of tuples, so both entries land and a lookup resolves to the first. log_processor relies on
    // this to keep a context key an action set twice rather than dropping one of the two values.
    let repeated = [("path".to_owned(), "/a".to_owned()), ("path".to_owned(), "/b".to_owned())];
    let entities = [MapEntity { id: "3", context: Map(&repeated), multi_context: Map(&[]), stats: Map(&[]) }];
    clickhouse.insert_borrowed::<MapEntity>("map_entity", &entities).await?;
    flush(&clickhouse).await?;

    let (value, entries) = clickhouse
        .select_one::<(String, u64)>(
            "SELECT context['path'], toUInt64(length(context)) FROM map_entity WHERE id = ?",
            &[&"3"],
        )
        .await?
        .unwrap();
    assert_eq!(value, "/a");
    assert_eq!(entries, 2);

    Ok(())
}
