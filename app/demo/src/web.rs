use std::sync::Arc;

use axum::Router;
use framework::exception::Exception;
use framework::validate::Validator;
use framework_macro::Validate;
use framework_macro::api;
use serde::Deserialize;
use serde::Serialize;
use tracing::warn;

use crate::AppState;

pub fn routes(state: &'static AppState) -> Router {
    let service = UserServiceImpl { _state: state };
    let service = Arc::new(service);

    user_service::route(service).with_state(state)
}

#[derive(Debug, Deserialize, Serialize, Validate)]
pub struct CreateUserRequest {
    #[not_blank]
    pub name: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SearchUserRequest {
    pub name: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SearchUserResponse {
    pub id: i64,
    pub name: String,
}

#[api]
pub trait UserService {
    #[post]
    #[path("/user/create")]
    async fn create(&self, request: CreateUserRequest) -> Result<(), Exception>;

    #[get]
    #[path("/user/search")]
    async fn search(&self, request: SearchUserRequest) -> Result<SearchUserResponse, Exception>;

    #[get]
    #[path("/user/get")]
    async fn get(&self) -> Result<SearchUserResponse, Exception>;
}

pub struct UserServiceImpl {
    _state: &'static AppState,
}

impl UserService for UserServiceImpl {
    async fn create(&self, request: CreateUserRequest) -> Result<(), Exception> {
        request.validate()?;
        warn!("trace");
        // use self.state if needed
        Ok(())
    }

    async fn search(&self, request: SearchUserRequest) -> Result<SearchUserResponse, Exception> {
        Ok(SearchUserResponse { id: 1, name: request.name })
    }

    async fn get(&self) -> Result<SearchUserResponse, Exception> {
        Ok(SearchUserResponse { id: 1, name: "something".to_owned() })
    }
}
