use std::fmt::Debug;
use std::sync::Arc;

use axum::Router;
use axum::debug_handler;
use axum::routing::get;
use framework::exception::Exception;
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

pub struct AppState {}

#[webservice]
pub trait UserService {
    #[post]
    #[path("/user/create")]
    fn create(&self, request: CreateUserRequest) -> impl Future<Output = Result<CreateUserResponse, Exception>> + Send;
}

mod user_service {
    use std::sync::Arc;

    use axum::Router;
    use axum::routing::MethodFilter;
    use axum::routing::on;
    use framework::exception::Exception;
    use framework::http::HttpClient;
    use framework::web::api::__into_response;
    use framework::web::api::ApiClient;
    use framework::web::body::Json;

    use crate::CreateUserRequest;
    use crate::CreateUserResponse;
    use crate::UserService;

    pub fn route<T>(service: Arc<T>) -> Router
    where
        T: UserService + Send + Sync + 'static,
    {
        let router = Router::new();
        let svc = Arc::clone(&service);
        let router = router.route(
            "/user/create",
            on(MethodFilter::POST, async move |Json(req): Json<CreateUserRequest>| {
                let result = svc.create(req).await;
                __into_response(result)
            }),
        );
        router
    }

    pub fn client(http_client: HttpClient, api_url: &'static str) -> impl UserService {
        struct Client {
            client: ApiClient,
        }
        impl UserService for Client {
            async fn create(&self, request: CreateUserRequest) -> Result<CreateUserResponse, Exception> {
                self.client.post("/user/create", request).await
            }
        }
        Client { client: ApiClient::new(http_client, api_url) }
    }
}

pub struct UserServiceImpl {
    state: Arc<AppState>,
}

impl UserService for UserServiceImpl {
    async fn create(&self, request: CreateUserRequest) -> Result<CreateUserResponse, Exception> {
        if request.name.is_empty() {
            return Err(validation_error!(message = "name must not be empty"));
        }
        // use self.state if needed
        Ok(CreateUserResponse { id: 1, name: request.name })
    }
}

// pub struct UserServiceClient;

// impl UserService for UserServiceClient {
//     async fn create(&self, request: CreateUserRequest) -> Result<CreateUserResponse, Exception>;
// }

impl UserServiceImpl {
    fn new() -> Self {
        UserServiceImpl { state: Arc::new(AppState {}) }
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

    let handle = task::spawn_task(async move {
        start_http_server(app, signal, HttpServerConfig::default()).await?;
        Ok(())
    });

    handle.await??;

    // tokio::time::sleep(Duration::from_secs(5)).await;

    // let client = client(HttpClient::default(), "http://localhost:8080");
    // let resp = client.create(CreateUserRequest { name: "yes".to_owned() }).await?;
    // println!("{resp:?}");

    Ok(())
}
