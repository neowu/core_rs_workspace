use std::fmt::Debug;
use std::ops::Deref;

use axum::extract::FromRequest;
use axum::extract::Request;
use axum::http::HeaderValue;
use axum::http::header;
use axum::response::IntoResponse;
use axum::response::Response;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tracing::debug;

use crate::exception::Exception;
use crate::exception::Severity;
use crate::exception::error_code;
use crate::json;
use crate::web::error::HttpError;

pub struct TextBody(pub String);

impl Deref for TextBody {
    type Target = String;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<S> FromRequest<S> for TextBody
where
    S: Send + Sync,
{
    type Rejection = HttpError;

    async fn from_request(request: Request, state: &S) -> Result<Self, Self::Rejection> {
        let result = String::from_request(request, state).await;
        match result {
            Ok(body) => {
                debug!("[request] body={body}");
                Ok(TextBody(body))
            }
            Err(rejection) => {
                let error_message = rejection.body_text();
                Err(exception!(
                    severity = Severity::Warn,
                    code = error_code::BAD_REQUEST,
                    message = format!("failed to read body, error={error_message}")
                )
                .into())
            }
        }
    }
}

pub struct Json<T>(pub T);

impl<S, T> FromRequest<S> for Json<T>
where
    S: Send + Sync,
    T: DeserializeOwned,
{
    type Rejection = HttpError;

    async fn from_request(request: Request, state: &S) -> Result<Self, Self::Rejection> {
        let result = String::from_request(request, state).await;
        match result {
            Ok(body) => {
                debug!("[request] body={body}");
                let body_object: Result<T, Exception> = json::from_json(&body);
                match body_object {
                    Ok(value) => Ok(Self(value)),
                    Err(exception) => Err(exception!(
                        severity = Severity::Warn,
                        code = error_code::BAD_REQUEST,
                        message = "failed to parse json body",
                        source = exception
                    )
                    .into()),
                }
            }
            Err(rejection) => {
                let error_message = rejection.body_text();
                Err(exception!(
                    severity = Severity::Warn,
                    code = error_code::BAD_REQUEST,
                    message = format!("failed to read body, error={error_message}")
                )
                .into())
            }
        }
    }
}

impl<T> IntoResponse for Json<T>
where
    T: Serialize + Debug,
{
    fn into_response(self) -> Response {
        let result = json::to_json(&self.0);
        match result {
            Ok(body) => {
                debug!("[response] body={body}");
                let length = body.len();
                debug!(response_content_length = length, "stats");
                (
                    [
                        (header::CONTENT_TYPE, HeaderValue::from_static("application/json")),
                        (header::CONTENT_LENGTH, HeaderValue::from_str(&format!("{}", length)).unwrap()),
                    ],
                    body,
                )
                    .into_response()
            }
            Err(exception) => HttpError::from(exception).into_response(),
        }
    }
}
