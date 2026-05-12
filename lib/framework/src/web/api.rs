use std::fmt::Debug;

use axum::response::IntoResponse as _;
use axum::response::Response;
use http::header;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::exception::Exception;
use crate::http::HttpClient;
use crate::http::HttpMethod;
use crate::http::HttpRequest;
use crate::http::HttpResponse;
use crate::json;
use crate::web::body::Json;
use crate::web::error::HttpError;
use crate::web::error::HttpErrorBody;

#[doc(hidden)] // disable auto complete, it's used by framework
pub fn __into_response<T>(result: Result<T, Exception>) -> Response
where
    T: Serialize + Debug,
{
    match result {
        Ok(response) => Json(response).into_response(),
        Err(err) => HttpError::from(err).into_response(),
    }
}

pub struct ApiClient {
    http_client: HttpClient,
    api_url: &'static str,
}

impl ApiClient {
    pub fn new(http_client: HttpClient, api_url: &'static str) -> Self {
        Self { http_client, api_url }
    }

    pub async fn post<Req, Res>(&self, path: &'static str, request: Req) -> Result<Res, Exception>
    where
        Req: Serialize + Debug,
        Res: DeserializeOwned,
    {
        let mut http_request = HttpRequest::new(HttpMethod::Post, format!("{}{path}", self.api_url));
        http_request.body(json::to_json(&request)?, "application/json".to_owned());
        let response = self.http_client.execute(http_request).await?;
        parse_response(&response)
    }

    // TODO: add retry
    pub async fn get<Req, Res>(&self, path: &'static str, request: Req) -> Result<Res, Exception>
    where
        Req: Serialize + Debug,
        Res: DeserializeOwned,
    {
        let query_string = serde_html_form::to_string(&request)?;
        let http_request = HttpRequest::new(HttpMethod::Get, format!("{}{path}?{query_string}", self.api_url));
        let response = self.http_client.execute(http_request).await?;
        parse_response(&response)
    }

    // TODO: add retry
    pub async fn put<Req, Res>(&self, path: &'static str, request: Req) -> Result<Res, Exception>
    where
        Req: Serialize + Debug,
        Res: DeserializeOwned,
    {
        let mut http_request = HttpRequest::new(HttpMethod::Put, format!("{}{path}", self.api_url));
        http_request.body(json::to_json(&request)?, "application/json".to_owned());
        let response = self.http_client.execute(http_request).await?;
        parse_response(&response)
    }
}

fn parse_response<Res>(response: &HttpResponse) -> Result<Res, Exception>
where
    Res: DeserializeOwned,
{
    let status = response.status;
    if status < 300 {
        json::from_json(&response.body)
    } else if let Some(content_type) = response.headers.get(&header::CONTENT_TYPE)
        && content_type == "application/json"
        && let Ok(error) = json::from_json::<HttpErrorBody>(&response.body)
    {
        if let Some(ref code) = error.code {
            Err(exception!(
                severity = error.severity,
                code = code,
                message = format!("failed to call api, status={status}, error={}", error.message)
            ))
        } else {
            Err(exception!(
                severity = error.severity,
                message = format!("failed to call api, status={status}, error={}", error.message)
            ))
        }
    } else {
        Err(exception!(message = format!("failed to call api, status={status}, body={}", response.body)))
    }
}
