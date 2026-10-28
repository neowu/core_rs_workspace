use std::collections::HashMap;
use std::result::Result;
use std::sync::Arc;

use framework::exception;
use framework::exception::Exception;
use framework::exception::error_code;
use framework::http::HeaderValue;
use framework::http::Method;
use framework::http::StatusCode;
use framework::http::header;
use framework::json;
use framework::log;
use framework::log::Severity;
use framework::string::StringExt as _;
use framework::time::DateTime;
use framework::validate::Validator;
use framework::validation_error;
use framework::warn;
use framework::web::request::Request;
use framework::web::response::Response;
use framework::web::router::Router;
use framework_macro::Validate;
use serde::Deserialize;
use serde::Serialize;

use crate::AppState;
use crate::kafka::EventMessage;

const EVENT_PATH: &str = "/event/";

pub(super) fn routes(state: Arc<AppState>) -> Router {
    Router::new()
        .state(state)
        .route(Method::GET, "/robots.txt", robots_txt)
        .route(Method::GET, "/1x1.png", png_1x1)
        .route(Method::OPTIONS, "/event/submit", event_submit_options)
        .route(Method::POST, "/event/submit", event_submit)
        .prefix(Method::OPTIONS, EVENT_PATH, event_options)
        .prefix(Method::POST, EVENT_PATH, event_post)
        .into()
}

async fn robots_txt(_state: Arc<AppState>, _request: Request) -> Result<Response, Exception> {
    Ok(Response::text("User-agent: *\nDisallow: /")
        .header(header::CACHE_CONTROL, HeaderValue::from_static("public, max-age=2592000"))) // 30 days
}

async fn png_1x1(_state: Arc<AppState>, _request: Request) -> Result<Response, Exception> {
    Ok(Response::bytes(&include_bytes!("../assets/1x1.png")[..], HeaderValue::from_static("image/png")))
}

async fn event_options(_state: Arc<AppState>, request: Request) -> Result<Response, Exception> {
    event_app(&request)?;
    cors_preflight(&request)
}

async fn event_submit_options(_state: Arc<AppState>, request: Request) -> Result<Response, Exception> {
    cors_preflight(&request)
}

fn cors_preflight(request: &Request) -> Result<Response, Exception> {
    let origin = request
        .headers()
        .get(header::ORIGIN)
        .ok_or_else(|| exception!("access denied", severity = Severity::Warn, code = error_code::FORBIDDEN))?;

    Ok(Response::empty()
        .status(StatusCode::OK)
        .header(header::ACCESS_CONTROL_ALLOW_ORIGIN, origin.clone())
        .header(header::ACCESS_CONTROL_ALLOW_METHODS, HeaderValue::from_static("POST, PUT, OPTIONS"))
        .header(header::ACCESS_CONTROL_ALLOW_HEADERS, HeaderValue::from_static("Accept, Content-Type"))
        .header(header::ACCESS_CONTROL_ALLOW_CREDENTIALS, HeaderValue::from_static("true")))
}

// event will be sent via ajax or navigator.sendBeacon(), refer to https://developer.mozilla.org/en-US/docs/Web/API/Navigator/sendBeacon
async fn event_post(state: Arc<AppState>, mut request: Request) -> Result<Response, Exception> {
    let app = event_app(&request)?.to_owned();
    let body = request.text().await?;
    if !body.is_empty() {
        let event_request: SendEventRequest = parse_body(&body)?;
        process_events(&state, &app, event_request.events, &request).await?;
    }
    Ok(event_response(&request))
}

// static path, app is passed in body
async fn event_submit(state: Arc<AppState>, mut request: Request) -> Result<Response, Exception> {
    let body = request.text().await?;
    let event_request: SubmitEventRequest = parse_body(&body)?;
    process_events(&state, &event_request.app, event_request.events, &request).await?;
    Ok(event_response(&request))
}

fn parse_body<T: for<'de> Deserialize<'de> + Validator>(body: &str) -> Result<T, Exception> {
    let request: T = json::from_json(body).map_err(|err| {
        exception!("failed to parse json body", severity = Severity::Warn, code = error_code::BAD_REQUEST, source = err)
    })?;
    request.validate()?;
    Ok(request)
}

fn event_response(request: &Request) -> Response {
    let mut response = Response::empty().status(StatusCode::OK);
    if let Some(origin) = request.headers().get(header::ORIGIN) {
        response = response
            .header(header::ACCESS_CONTROL_ALLOW_ORIGIN, origin.clone())
            .header(header::ACCESS_CONTROL_ALLOW_CREDENTIALS, HeaderValue::from_static("true"));
    }
    response
}

// same as the former `/event/{app}` route, one non empty segment
fn event_app(request: &Request) -> Result<&str, Exception> {
    request.path().strip_prefix(EVENT_PATH).filter(|app| !app.is_empty() && !app.contains('/')).ok_or_else(|| {
        exception!(
            format!("not found, path={}", request.path()),
            severity = Severity::Warn,
            code = error_code::NOT_FOUND
        )
    })
}

async fn process_events(state: &AppState, app: &str, events: Vec<Event>, request: &Request) -> Result<(), Exception> {
    let now = DateTime::now();
    for event in events {
        if let Err(error) = event.custom_validate() {
            warn!(error_code = "INVALID_EVENT", "skip invalid event, error={error}");
            continue;
        }

        let mut message = EventMessage {
            id: log::id_generator::next_id(now.unix_timestamp_millis()),
            timestamp: now,
            app: app.to_owned(),
            client_timestamp: event.date,
            result: json::to_json_value(&event.result),
            action: event.action,
            error_code: event.error_code,
            error_message: event.error_message,
            elapsed: event.elapsed_time,
            context: event.context,
            stats: event.stats,
            info: event.info,
        };

        if let Some(user_agent) = request.user_agent() {
            message.context.insert("user_agent".to_owned(), user_agent.to_owned());
        }

        message.context.insert("client_ip".to_owned(), request.client_ip().to_owned());

        state.producer.send(&state.topics.event, None, &message).await?;
    }

    Ok(())
}

#[derive(Validate, Deserialize, Debug)]
struct SendEventRequest {
    #[length(min = 1)]
    events: Vec<Event>,
}

#[derive(Validate, Deserialize, Debug)]
struct SubmitEventRequest {
    #[length(min = 1, max = 20)]
    app: String,
    #[length(min = 1)]
    events: Vec<Event>,
}

#[derive(Validate, Deserialize, Debug)]
struct Event {
    date: DateTime,
    result: EventResult,
    #[length(max = 200)]
    action: String,
    #[length(max = 200)]
    #[serde(rename = "errorCode")]
    error_code: Option<String>,
    #[length(max = 1000)]
    #[serde(rename = "errorMessage")]
    error_message: Option<String>,
    context: HashMap<String, String>,
    stats: Option<HashMap<String, f64>>,
    info: Option<HashMap<String, String>>,
    #[serde(rename = "elapsedTime")]
    elapsed_time: i64,
}

#[derive(Serialize, Deserialize, Debug)]
enum EventResult {
    #[serde(rename = "OK")]
    Ok,
    #[serde(rename = "WARN")]
    Warn,
    #[serde(rename = "ERROR")]
    Error,
}

impl Event {
    const MAX_KEY_LENGTH: usize = 50;
    const MAX_CONTEXT_VALUE_LENGTH: usize = 1000;
    const MAX_INFO_VALUE_LENGTH: usize = 500_000;
    const MAX_ESTIMATED_LENGTH: usize = 900_000; // by default kafka message limit is 1M, leave 100k for rest of message

    fn custom_validate(&self) -> Result<(), Exception> {
        self.validate()?;

        // Validate action for OK result
        if matches!(self.result, EventResult::Ok) && self.action.is_empty() {
            return Err(validation_error!("action must not be empty if result is OK"));
        }

        if (matches!(self.result, EventResult::Warn) || matches!(self.result, EventResult::Error))
            && self.error_code.as_ref().is_none_or(String::is_empty)
        {
            return Err(validation_error!("errorCode must not be empty if result is WARN/ERROR"));
        }

        // Validate maps and estimate size
        let mut estimated_length = 0;
        estimated_length += Event::validate_map(&self.context, Event::MAX_KEY_LENGTH, Event::MAX_CONTEXT_VALUE_LENGTH)?;
        if let Some(info) = &self.info {
            estimated_length += Event::validate_map(info, Event::MAX_KEY_LENGTH, Event::MAX_INFO_VALUE_LENGTH)?;
        }
        if let Some(stats) = &self.stats {
            estimated_length += Event::validate_stats(stats, Event::MAX_KEY_LENGTH)?;
        }
        if estimated_length > Event::MAX_ESTIMATED_LENGTH {
            return Err(validation_error!(format!("event is too large, estimatedLength={estimated_length}")));
        }

        Ok(())
    }

    fn validate_map(
        map: &HashMap<String, String>,
        max_key_length: usize,
        max_value_length: usize,
    ) -> Result<usize, Exception> {
        let mut estimated_length = 0;
        for (key, value) in map {
            if key.len() > max_key_length {
                let truncated = key.truncate_to_max(50);
                return Err(validation_error!(format!("key is too long, key={truncated}...(truncated)")));
            }
            estimated_length += key.len();

            if value.len() > max_value_length {
                let truncated = value.truncate_to_max(200);
                return Err(validation_error!(format!(
                    "value is too long, key={key}, value={truncated}...(truncated)"
                )));
            }
            estimated_length += value.len();
        }
        Ok(estimated_length)
    }

    fn validate_stats(stats: &HashMap<String, f64>, max_key_length: usize) -> Result<usize, Exception> {
        let mut estimated_length = 0;
        for key in stats.keys() {
            if key.len() > max_key_length {
                let truncated = key.truncate_to_max(50);
                return Err(validation_error!(format!("key is too long, key={truncated}...(truncated)")));
            }
            estimated_length += key.len() + 5; // estimate double value as 5 chars
        }
        Ok(estimated_length)
    }
}
