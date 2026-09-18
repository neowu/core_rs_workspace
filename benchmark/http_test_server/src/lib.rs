//! The contract, and the only reason this crate has a lib target: `http_test_client` shares these
//! payload types so request and response shapes cannot drift between the two processes.

use framework::exception::Exception;
use framework_macro::api;
use serde::Deserialize;
use serde::Serialize;

/// Query of the get endpoints, one scalar so query parsing is not the subject.
#[derive(Debug, Serialize, Deserialize)]
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
#[derive(Debug, Serialize, Deserialize)]
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

/// Same payloads and same work as the plain controllers, on a different path. The delta between the
/// two is the cost of the `#[api]` generated route.
#[api]
pub trait BenchmarkService {
    #[get]
    #[path("/benchmark/api/get")]
    async fn get(&self, request: GetRequest) -> Result<GetResponse, Exception>;

    #[post]
    #[path("/benchmark/api/post")]
    async fn post(&self, request: PostRequest) -> Result<PostResponse, Exception>;
}
