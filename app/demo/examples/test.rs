use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::debug_handler;
use axum::routing::get;
use framework::exception::Exception;
use framework::http::HttpClient;
use framework::log;
use framework::log::appender::ConsoleAppender;
use framework::shutdown::Shutdown;
use framework::task;
use framework::validation_error;
use framework::web::body::Json;
use framework::web::body::Query;
use framework::web::error::HttpResult;
use framework::web::server::HttpServerConfig;
use framework::web::server::start_http_server;
use framework_macro::webservice;
use serde::Deserialize;
use serde::Serialize;
use tracing::warn;

#[derive(Debug, Deserialize, Serialize)]
pub struct CreateUserRequest {
    name: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CreateUserResponse {
    id: i64,
    name: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SearchUserRequest {
    name: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SearchUserResponse {
    id: i64,
    name: String,
}

pub struct AppState {}

#[webservice]
pub trait UserService {
    #[post] // only support get / put and post
    #[path("/user/create")]
    async fn create(&self, request: CreateUserRequest) -> Result<CreateUserResponse, Exception>;

    #[get]
    #[path("/user/search")]
    async fn search(&self, request: SearchUserRequest) -> Result<SearchUserResponse, Exception>;
}

pub struct UserServiceImpl {
    _state: Arc<AppState>,
}

impl UserService for UserServiceImpl {
    async fn create(&self, request: CreateUserRequest) -> Result<CreateUserResponse, Exception> {
        if request.name.is_empty() {
            return Err(validation_error!(message = "name must not be empty"));
        }
        // use self.state if needed
        Ok(CreateUserResponse { id: 1, name: request.name })
    }

    async fn search(&self, request: SearchUserRequest) -> Result<SearchUserResponse, Exception> {
        Ok(SearchUserResponse { id: 1, name: request.name })
    }
}

// pub struct UserServiceClient;

// impl UserService for UserServiceClient {
//     async fn create(&self, request: CreateUserRequest) -> Result<CreateUserResponse, Exception>;
// }

impl UserServiceImpl {
    fn new() -> Self {
        UserServiceImpl { _state: Arc::new(AppState {}) }
    }
}

#[debug_handler]
async fn test(Query(param): Query<CreateUserRequest>) -> HttpResult<Json<CreateUserResponse>> {
    warn!("trigger");
    Ok(Json(CreateUserResponse { id: 1, name: param.name }))
}

#[tokio::main]
async fn main() -> Result<(), Exception> {
    log::init_with_action(ConsoleAppender);

    let shutdown = Shutdown::new();
    let signal = shutdown.subscribe();
    shutdown.listen();

    let service = UserServiceImpl::new();
    let service = Arc::new(service);

    let app = Router::new();
    let app = app.merge(user_service::route(service));
    let app = app.route("/test", get(test));

    task::spawn_task(async move {
        start_http_server(app, signal, HttpServerConfig::default()).await?;
        Ok(())
    });

    tokio::time::sleep(Duration::from_secs(5)).await;

    let client = user_service::client(HttpClient::default(), "http://localhost:8080");
    let resp = client.create(CreateUserRequest { name: "yes".to_owned() }).await?;
    println!("{resp:?}");

    Ok(())
}
