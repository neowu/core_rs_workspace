use std::env;
use std::sync::Arc;

use axum::Router;
use axum::extract::State;
use framework::exception;
use framework::exception::error_code::NOT_FOUND;
use framework::web::body::Json;
use framework::web::body::Query;
use framework::web::error::HttpResult;
use framework::web::route::get;
use framework::web::route::post;
use framework::web::route::put;
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

pub(crate) fn route(database: Arc<Database>) -> Router {
    Router::new()
        .route("/benchmark/init_db", put(init_db))
        .route("/benchmark/db/select", get(select))
        .route("/benchmark/db/insert_ignore", post(insert_ignore))
        .with_state(database)
}

async fn init_db(
    State(db): State<Arc<Database>>,
    Json(request): Json<InitDbRequest>,
) -> HttpResult<Json<InitDbResponse>> {
    database::execute(&db, "DROP TABLE IF EXISTS \"benchmark_entity\"", &[]).await?;
    database::execute(
        &db,
        "CREATE TABLE \"benchmark_entity\" (
            id      BIGINT PRIMARY KEY,
            name    TEXT NOT NULL,
            amount  BIGINT NOT NULL
        )",
        &[],
    )
    .await?;
    for id in 1..=request.rows {
        repository::insert(&db, &BenchmarkEntity { id, name: format!("name-{id}"), amount: id * 100 }).await?;
    }
    Ok(Json(InitDbResponse { rows: request.rows }))
}

async fn select(
    State(db): State<Arc<Database>>,
    Query(request): Query<GetRequest>,
) -> HttpResult<Json<DbSelectResponse>> {
    let entity = repository::select_one(&db, vec![BenchmarkEntity::ID.eq(request.id)])
        .await?
        .ok_or_else(|| exception!(format!("entity not found, id={}", request.id), code = NOT_FOUND))?;
    Ok(Json(DbSelectResponse { id: entity.id, name: entity.name, amount: entity.amount }))
}

async fn insert_ignore(
    State(db): State<Arc<Database>>,
    Json(request): Json<DbInsertRequest>,
) -> HttpResult<Json<DbInsertResponse>> {
    let entity = BenchmarkEntity { id: request.id, name: request.name, amount: request.amount };
    let inserted = repository::insert_ignore(&db, &entity).await?;
    Ok(Json(DbInsertResponse { id: entity.id, inserted }))
}
