use std::sync::Arc;

use axum::Router;
use chrono::Utc;
use framework::exception::Exception;
use framework::validate::Validator as _;
use framework_db::Json;
use framework_db::repository;
use uuid::Uuid;

use crate::AppState;
use crate::user::CreateUserRequest;
use crate::user::GetUserByNameRequest;
use crate::user::GetUserResponse;
use crate::user::UpdateUserRequest;
use crate::user::User;
use crate::user::UserService;
use crate::user::user_service;

pub fn routes(state: &'static AppState) -> Router {
    let service = UserServiceImpl { state };
    user_service::route(Arc::new(service))
}

struct UserServiceImpl {
    state: &'static AppState,
}

impl UserService for UserServiceImpl {
    async fn create(&self, request: CreateUserRequest) -> Result<Uuid, Exception> {
        request.validate()?;

        let user = User {
            id: Uuid::now_v7(),
            name: request.name,
            rating: request.rating,
            tags: Json(vec![]),
            created_date: Utc::now(),
        };

        repository::insert(&self.state.db, &user).await?;

        Ok(user.id)
    }

    async fn get_by_name(&self, request: GetUserByNameRequest) -> Result<Option<GetUserResponse>, Exception> {
        request.validate()?;

        let user = repository::select_one(&self.state.db, vec![User::FIELD_NAME.eq(&request.name)]).await?;

        Ok(user.map(|user| GetUserResponse { id: user.id, name: user.name, rating: user.rating, tags: user.tags.0 }))
    }

    async fn update(&self, request: UpdateUserRequest) -> Result<(), Exception> {
        let mut updates = vec![];
        if request.rating.is_some() {
            updates.push(User::FIELD_RATING.update(request.rating));
        }
        if let Some(tags) = request.tags {
            updates.push(User::FIELD_TAGS.update(Json(tags)));
        }
        repository::update(&self.state.db, &request.id, updates).await?;
        Ok(())
    }

    async fn get_test(&self, request: GetUserByNameRequest) -> Result<GetUserResponse, Exception> {
        Ok(GetUserResponse { id: Uuid::now_v7(), name: request.name, rating: None, tags: vec![] })
    }
}
