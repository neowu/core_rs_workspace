use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use serde::Deserialize;
use serde::Serialize;

use crate::exception::Exception;
use crate::exception::Severity;
use crate::exception::error_code;
use crate::log;
use crate::web::body::Json;

pub type HttpResult<T> = Result<T, HttpError>;

#[derive(Debug)]
pub struct HttpError {
    status_code: StatusCode,
    body: HttpErrorBody,
}

#[derive(Debug, Serialize, Deserialize)]
struct HttpErrorBody {
    severity: Severity,
    code: Option<String>,
    message: String,
}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        (self.status_code, Json(self.body)).into_response()
    }
}

impl<E> From<E> for HttpError
where
    E: Into<Exception>,
{
    fn from(err: E) -> Self {
        let exception: Exception = err.into();
        log::log_exception(&exception);

        let status_code = exception.code.as_deref().map_or(StatusCode::INTERNAL_SERVER_ERROR, |code| match code {
            error_code::BAD_REQUEST | error_code::VALIDATION_ERROR => StatusCode::BAD_REQUEST,
            error_code::NOT_FOUND => StatusCode::NOT_FOUND,
            error_code::FORDIDDEN => StatusCode::FORBIDDEN,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        });

        Self {
            status_code,
            body: HttpErrorBody { severity: exception.severity, code: exception.code, message: exception.message },
        }
    }
}
