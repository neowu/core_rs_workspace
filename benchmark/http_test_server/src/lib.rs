//! The contract, and the only reason this crate has a lib target: `http_test_client` shares these
//! payload types and the info endpoint's shape so they cannot drift between the two processes.

use framework::exception::Exception;
use framework_macro::Validate;
use framework_macro::api;
use serde::Deserialize;
use serde::Serialize;

pub mod info;

use crate::info::ServerInfo;

/// Query of the get endpoints, one scalar so query parsing is not the subject.
#[derive(Debug, Serialize, Deserialize, Validate)]
pub struct GetRequest {
    pub id: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetResponse {
    pub id: i64,
    pub name: String,
    pub values: Vec<i64>,
}

/// Body of the post endpoints, `values` is sized by the client to vary the body size.
#[derive(Debug, Serialize, Deserialize, Validate)]
pub struct PostRequest {
    pub id: i64,
    pub name: String,
    pub values: Vec<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PostResponse {
    pub id: i64,
    pub sum: i64,
}

// the plain controllers and the #[api] ones share these, so the two routes differ only in the
// framework code between the socket and the call, never in the work done
impl GetResponse {
    pub fn new(request: &GetRequest) -> Self {
        GetResponse { id: request.id, name: "benchmark".to_owned(), values: vec![request.id; 10] }
    }
}

impl PostResponse {
    pub fn new(request: &PostRequest) -> Self {
        PostResponse { id: request.id, sum: request.values.iter().sum() }
    }
}

/// Body of `PUT /benchmark/init_db`: drops and recreates the table, then seeds ids `1..=rows`.
#[derive(Debug, Serialize, Deserialize, Validate)]
pub struct InitDbRequest {
    pub rows: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InitDbResponse {
    pub rows: i64,
}

/// Response of `GET /benchmark/db/select?id=`, one row by primary key.
#[derive(Debug, Serialize, Deserialize)]
pub struct DbSelectResponse {
    pub id: i64,
    pub name: String,
    pub amount: i64,
}

/// Body of `POST /benchmark/db/insert_ignore`, the client repeats one id so only the first insert
/// lands and the table never grows.
#[derive(Debug, Serialize, Deserialize, Validate)]
pub struct DbInsertRequest {
    pub id: i64,
    pub name: String,
    pub amount: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DbInsertResponse {
    pub id: i64,
    pub inserted: bool,
}

/// Generated API routes for the HTTP and database benchmarks.
#[api]
pub trait BenchmarkService {
    #[get]
    #[path("/benchmark/api/get")]
    async fn get(&self, request: GetRequest) -> Result<GetResponse, Exception>;

    #[post]
    #[path("/benchmark/api/post")]
    async fn post(&self, request: PostRequest) -> Result<PostResponse, Exception>;

    #[get]
    #[path("/benchmark/info")]
    async fn info(&self) -> Result<ServerInfo, Exception>;

    #[put]
    #[path("/benchmark/init_db")]
    async fn init_db(&self, request: InitDbRequest) -> Result<InitDbResponse, Exception>;

    #[get]
    #[path("/benchmark/db/select")]
    async fn db_select(&self, request: GetRequest) -> Result<DbSelectResponse, Exception>;

    #[post]
    #[path("/benchmark/db/insert_ignore")]
    async fn db_insert_ignore(&self, request: DbInsertRequest) -> Result<DbInsertResponse, Exception>;
}
