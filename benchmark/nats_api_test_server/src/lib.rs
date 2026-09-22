//! The contract, and the only reason this crate has a lib target: `nats_api_test_client` shares
//! these payload types and subjects so request and response shapes cannot drift between the two
//! processes.

use framework::exception::Exception;
use framework_macro::nats_api;
use serde::Deserialize;
use serde::Serialize;

// the #[subject] attribute takes a literal, so these repeat it for the client to address; they sit
// beside the trait so the two cannot be changed apart
pub const GET: &str = "api.benchmark.get";
pub const POST: &str = "api.benchmark.post";

/// Request of the get subjects, one scalar so decoding is not the subject.
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

/// Request of the post subjects, `values` is sized by the client to vary the payload size.
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

/// The service under test. `#[nats_api]` is the only way a service is built, so it is the only
/// thing there is to measure.
#[nats_api]
pub trait BenchmarkService {
    #[subject = "api.benchmark.get"]
    async fn get(&self, request: GetRequest) -> Result<GetResponse, Exception>;

    #[subject = "api.benchmark.post"]
    async fn post(&self, request: PostRequest) -> Result<PostResponse, Exception>;
}
