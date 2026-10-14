use framework::exception::Exception;
use framework_db::Database;
use framework_db::DbConfig;
use framework_db::database;
use framework_macro::integration_test;

#[integration_test]
async fn error() -> Result<(), Exception> {
    // nothing listens on port 1, connect fails fast
    let db = Database::new(DbConfig {
        uri: "postgres://postgres.test:1/missing_db".to_owned(),
        user: "postgres".to_owned(),
        password: "postgres".to_owned(),
        client: env!("CARGO_PKG_NAME"),
        pool_max_size: 1,
        ..DbConfig::default()
    });

    let error = database::execute(&db, "SELECT 1", &[]).await.unwrap_err();
    assert_eq!(error.code, Some("DB_ERROR"));
    assert_eq!(error.message, "failed to get db connection, uri=postgres://postgres.test:1/missing_db");

    let db = db_test::client();
    let error = database::execute(&db, "SELECT * FROM missing_table", &[]).await.unwrap_err();
    assert_eq!(error.code, Some("DB_ERROR"));
    assert_eq!(error.message, "failed to call db, uri=postgres://postgres.test:5432/postgres");

    Ok(())
}
