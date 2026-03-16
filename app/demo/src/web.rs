use axum::Router;
use axum::debug_handler;
use axum::routing::post;
use framework::exception::Exception;
use framework::validation_error;
use framework::web::body::Json;
// use framework::web::body::TextBody;
use framework::web::error::HttpResult;
use serde::Deserialize;
use serde::Serialize;
use tracing::warn;

use crate::AppState;

pub fn routes() -> Router<&'static AppState> {
    Router::new().route("/hello", post(hello))
}

#[derive(Debug, Deserialize)]
struct HelloRequest {
    message: String,
}

impl HelloRequest {
    fn validate(&self) -> Result<(), Exception> {
        if self.message.len() > 10 {
            let exception = validation_error!(message = "message len must less than 10");
            warn!("test log, error={exception:?}");
            return Err(exception);
        }
        Ok(())
    }
}

#[derive(Debug, Serialize)]
struct HelloResponse {
    message: String,
}

#[debug_handler]
async fn hello(Json(request): Json<Option<HelloRequest>>) -> HttpResult<Json<HelloResponse>> {
    if let Some(request) = request {
        request.validate()?;
        return Ok(Json(HelloResponse {
            message: request.message,
        }));
    }

    Ok(Json(HelloResponse {
        message: "other".to_string(),
    }))
}

// #[debug_handler]
// async fn hello2(body: TextBody) -> HttpResult<Json<HelloResponse>> {
//     println!("!!! {}", body.0);

//     warn!("test");

//     Ok(Json(HelloResponse {
//         message: "hello".to_string(),
//     }))
// }
