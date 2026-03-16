// use std::sync::Arc;

// use axum::Json;
// use axum::Router;
// use axum::debug_handler;
// use axum::extract::State;
// use axum::routing::put;
// use framework::exception::Exception;
// use framework::web::error::HttpError;
// use serde::Deserialize;
// use serde::Serialize;
// use tokio::net::TcpListener;

// struct AppState;

// #[derive(Deserialize, Debug)]
// struct EchoRequest {
//     name: String,
// }

// #[derive(Serialize, Deserialize, Debug)]
// struct EchoResponse {
//     name: String,
// }

// struct EchoWebServiceClient {}

// impl EchoWebService for EchoWebServiceClient {
//     async fn echo(&self, request: EchoRequest) -> Result<EchoResponse, Exception> {
//         todo!()
//     }
// }

// // #[api]
// trait EchoWebService: Send + Sync + 'static {
//     // #[method(PUT)]
//     // #[path("/echo")]
//     async fn echo(&self, request: EchoRequest) -> Result<EchoResponse, Exception>;
// }

// #[debug_handler]
// async fn echo(
//     State(state): State<Arc<AppState>>,
//     Json(request): Json<EchoRequest>,
// ) -> Result<Json<EchoResponse>, HttpError> {
//     Ok(Json(EchoResponse { name: request.name }))
// }

// struct ApiState {
//     echo: EchoWebServiceClient,
// }

// struct EchoWebServiceImpl {
//     state: Arc<AppState>,
// }

// #[tokio::main]
// pub async fn main() -> Result<(), Exception> {
//     let state = Arc::new(AppState {});

//     let api = EchoWebServiceImpl { state: state.clone() };

//     let router = Router::new();
//     router.route("/echo", put(echo));

//     // TODO: register api into router

//     let listener = TcpListener::bind("0.0.0.0:8080").await?;
//     axum::serve(listener, app).await.unwrap();
//     Ok(())
// }

// fn register_echo_api(router: Router, echo: Box<impl EchoWebService>) -> Router {
//     let api_state = ApiState { echo };

//     router.route("/echo", put(echo_handler));

//     router
// }

// async fn echo_handler(
//     State(state): State<AppState>,
//     Json(req): Json<EchoRequest>,
// ) -> Result<Json<EchoResponse>, Exception> {
//     let resp = state.echo.echo(req).await?;
//     Ok(Json(resp))
// }

pub fn main() {
    let x = [0x73, 0xef, 0x17, 0x6d];
    println!("{:?}", x);
}
