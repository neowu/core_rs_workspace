use std::any::TypeId;
use std::fmt::Debug;
use std::mem::transmute_copy;

use axum::response::IntoResponse as _;
use axum::response::Response;
use http::Method;
use http::StatusCode;
use http::header;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::exception::Exception;
use crate::http::HttpClient;
use crate::http::HttpRequest;
use crate::http::HttpResponse;
use crate::json;
use crate::log::current_action_id;
use crate::web::CLIENT;
use crate::web::REF_ID;
use crate::web::body::Json;
use crate::web::error::HttpError;
use crate::web::error::HttpErrorBody;

#[doc(hidden)] // disable auto complete, it's used by framework
#[inline]
pub fn __into_response<T>(result: Result<T, Exception>) -> Response
where
    T: Serialize + Debug + 'static,
{
    match result {
        Ok(response) => {
            if TypeId::of::<T>() == TypeId::of::<()>() {
                StatusCode::NO_CONTENT.into_response()
            } else {
                Json(response).into_response()
            }
        }
        Err(err) => HttpError::from(err).into_response(),
    }
}

pub struct ApiClient {
    http_client: HttpClient,
    api_url: String,
    client: &'static str,
}

impl ApiClient {
    #[inline]
    pub const fn new(http_client: HttpClient, api_url: String, client: &'static str) -> Self {
        Self { http_client, api_url, client }
    }

    // TODO: add current action id
    #[inline]
    pub async fn get<Req, Res>(&self, path: &'static str, request: Req) -> Result<Res, Exception>
    where
        Req: Serialize + Debug + 'static,
        Res: DeserializeOwned + 'static,
    {
        let query_string = serde_html_form::to_string(&request)?;
        let url = if query_string.is_empty() {
            format!("{}{path}", self.api_url)
        } else {
            format!("{}{path}?{query_string}", self.api_url)
        };
        let mut http_request = HttpRequest::new(Method::GET, url);
        self.link_context(&mut http_request)?;
        let response = self.http_client.execute(http_request).await?;
        parse_response(&response)
    }

    #[inline]
    pub async fn post<Req, Res>(&self, path: &'static str, request: Req) -> Result<Res, Exception>
    where
        Req: Serialize + Debug,
        Res: DeserializeOwned + 'static,
    {
        let mut http_request = HttpRequest::new(Method::POST, format!("{}{path}", self.api_url));
        http_request.body(json::to_json(&request)?, "application/json");
        self.link_context(&mut http_request)?;
        let response = self.http_client.execute(http_request).await?;
        parse_response(&response)
    }

    #[inline]
    pub async fn put<Req, Res>(&self, path: &'static str, request: Req) -> Result<Res, Exception>
    where
        Req: Serialize + Debug,
        Res: DeserializeOwned + 'static,
    {
        let mut http_request = HttpRequest::new(Method::PUT, format!("{}{path}", self.api_url));
        http_request.body(json::to_json(&request)?, "application/json");
        self.link_context(&mut http_request)?;
        let response = self.http_client.execute(http_request).await?;
        parse_response(&response)
    }

    fn link_context(&self, http_request: &mut HttpRequest) -> Result<(), Exception> {
        if let Some(action_id) = current_action_id() {
            http_request.header(REF_ID, &action_id)?;
        }
        http_request.header(CLIENT, self.client)?;
        Ok(())
    }
}

fn parse_response<Res>(response: &HttpResponse) -> Result<Res, Exception>
where
    Res: DeserializeOwned + 'static,
{
    let status = response.status;
    if status < 300 {
        if TypeId::of::<Res>() == TypeId::of::<()>() {
            // SAFETY: We've verified Res is () via TypeId, so this transmute is sound.
            Ok(unsafe { transmute_copy(&()) })
        } else {
            json::from_json(&response.body)
        }
    } else if let Some(content_type) = response.headers.get(&header::CONTENT_TYPE)
        && content_type == "application/json"
        && let Ok(error) = json::from_json::<HttpErrorBody>(&response.body)
    {
        if let Some(ref code) = error.code {
            Err(exception!(
                format!("failed to call api, status={status}, error={}", error.message),
                severity = error.severity,
                code = code
            ))
        } else {
            Err(exception!(
                format!("failed to call api, status={status}, error={}", error.message),
                severity = error.severity
            ))
        }
    } else {
        Err(exception!(format!("failed to call api, status={status}, body={}", response.body)))
    }
}
