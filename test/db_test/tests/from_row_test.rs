use db_test::client;
use framework::exception::Exception;
use framework_db::database;
use framework_macro::FromRow;
use framework_macro::integration_test;

#[derive(FromRow, Debug, PartialEq)]
struct TableStats {
    #[column(name = "name")]
    name: String,
    #[column(name = "type")]
    kind: String,
    #[column(name = "rows")]
    rows: i64,
    #[column(name = "comment")]
    comment: Option<String>,
}

#[integration_test]
async fn from_row() -> Result<(), Exception> {
    let db = client();

    let stats: Vec<TableStats> = database::select(
        &db,
        "SELECT t.name, 'table' AS type, t.rows, NULL::text AS comment
         FROM (VALUES ('a', 1::bigint), ('b', 2::bigint)) AS t(name, rows)
         ORDER BY t.name",
        &[],
    )
    .await?;

    assert_eq!(
        stats,
        vec![
            TableStats { name: "a".to_owned(), kind: "table".to_owned(), rows: 1, comment: None },
            TableStats { name: "b".to_owned(), kind: "table".to_owned(), rows: 2, comment: None },
        ]
    );

    // a missing column fails the mapping instead of defaulting
    let result: Result<Vec<TableStats>, Exception> =
        database::select(&db, "SELECT 'a' AS name, 'table' AS type, 1::bigint AS rows", &[]).await;
    assert_eq!(result.unwrap_err().code, Some("DB_ERROR"));

    Ok(())
}
