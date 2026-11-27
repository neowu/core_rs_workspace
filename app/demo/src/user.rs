use framework::exception::Exception;
use framework_macro::Entity;
use framework_macro::Validate;
use framework_macro::api;
use framework_postgres::Json;
use framework_postgres::types::Timestamp;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

pub mod web;

#[derive(Entity, Debug)]
#[table(name = "user")]
struct User {
    #[primary_key]
    #[column(name = "id")]
    id: Uuid,
    #[column(name = "name")]
    name: String,
    #[column(name = "rating")]
    rating: Option<i32>,
    #[column(name = "tags")]
    tags: Json<Vec<String>>,
    #[column(name = "created_date")]
    created_date: Timestamp,
}

#[derive(Debug, Deserialize, Serialize, Validate)]
pub struct CreateUserRequest {
    #[not_blank]
    pub name: String,
    #[range(min = 0)]
    pub rating: Option<i32>,
}

#[derive(Debug, Deserialize, Serialize, Validate)]
pub struct GetUserByNameRequest {
    #[not_blank]
    pub name: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct GetUserResponse {
    id: Uuid,
    name: String,
    rating: Option<i32>,
    tags: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize, Validate)]
pub struct UpdateUserRequest {
    pub id: Uuid,
    pub rating: Option<i32>,
    pub tags: Option<Vec<String>>,
}

#[api]
pub trait UserService {
    #[post]
    #[path("/user/create")]
    async fn create(&self, request: CreateUserRequest) -> Result<Uuid, Exception>;

    #[get]
    #[path("/user/get_by_name")]
    async fn get_by_name(&self, request: GetUserByNameRequest) -> Result<Option<GetUserResponse>, Exception>;

    #[put]
    #[path("/user/update")]
    async fn update(&self, request: UpdateUserRequest) -> Result<(), Exception>;

    #[get]
    #[path("/get_test")]
    async fn get_test(&self, request: GetUserByNameRequest) -> Result<GetUserResponse, Exception>;
}
