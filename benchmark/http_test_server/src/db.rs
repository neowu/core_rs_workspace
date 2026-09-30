use std::env;

use framework::exception;
use framework::exception::Exception;
use framework::exception::error_code::NOT_FOUND;
use framework_db::Database;
use framework_db::DbConfig;
use framework_db::database;
use framework_db::repository;
use framework_macro::Entity;
use http_test_server::DbInsertRequest;
use http_test_server::DbInsertResponse;
use http_test_server::DbSelectResponse;
use http_test_server::GetRequest;
use http_test_server::InitDbRequest;
use http_test_server::InitDbResponse;

// postgres runs on the server host with trust auth
const DEFAULT_URL: &str = "postgres://localhost:5432/postgres";

#[derive(Entity, Debug)]
#[table(name = "benchmark_entity")]
struct BenchmarkEntity {
    #[primary_key]
    #[column(name = "id")]
    id: i64,
    #[column(name = "name")]
    name: String,
    #[column(name = "amount")]
    amount: i64,
}

pub(crate) fn database() -> Database {
    Database::new(DbConfig {
        uri: env::var("DB_URL").unwrap_or_else(|_| DEFAULT_URL.to_owned()),
        user: "postgres".to_owned(),
        password: String::new(),
        client: env!("CARGO_PKG_NAME"),
    })
}

pub(crate) async fn init_db(db: &Database, request: InitDbRequest) -> Result<InitDbResponse, Exception> {
    database::execute(db, "DROP TABLE IF EXISTS \"benchmark_entity\"", &[]).await?;
    database::execute(
        db,
        "CREATE TABLE \"benchmark_entity\" (
            id      BIGINT PRIMARY KEY,
            name    TEXT NOT NULL,
            amount  BIGINT NOT NULL
        )",
        &[],
    )
    .await?;
    for id in 1..=request.rows {
        repository::insert(db, &BenchmarkEntity { id, name: format!("name-{id}"), amount: id * 100 }).await?;
    }
    Ok(InitDbResponse { rows: request.rows })
}

pub(crate) async fn select(db: &Database, request: GetRequest) -> Result<DbSelectResponse, Exception> {
    let entity = repository::select_one(db, vec![BenchmarkEntity::ID.eq(request.id)])
        .await?
        .ok_or_else(|| exception!(format!("entity not found, id={}", request.id), code = NOT_FOUND))?;
    Ok(DbSelectResponse { id: entity.id, name: entity.name, amount: entity.amount })
}

pub(crate) async fn insert_ignore(db: &Database, request: DbInsertRequest) -> Result<DbInsertResponse, Exception> {
    let entity = BenchmarkEntity { id: request.id, name: request.name, amount: request.amount };
    let inserted = repository::insert_ignore(db, &entity).await?;
    Ok(DbInsertResponse { id: entity.id, inserted })
}
