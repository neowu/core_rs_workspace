use std::collections::HashMap;
use std::sync::Arc;

use axum::Extension;
use axum::Router;
use axum::debug_handler;
use axum::extract::Path;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::http::header;
use axum::routing::get;
use axum::routing::options;
use axum::routing::post;
use chrono::DateTime;
use chrono::Utc;
use framework::exception;
use framework::exception::CoreRsResult;
use framework::exception::Severity;
use framework::exception::error_code;
use framework::json;
use framework::log;
use framework::validation_error;
use framework::web::client_info::ClientInfo;
use framework::web::error::HttpResult;
use serde::Deserialize;
use serde::Serialize;
use tracing::warn;

use crate::AppState;
use crate::kafka::EventMessage;

pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/robots.txt", get(robots_txt))
        .route("/event/{app}", options(event_options))
        .route("/event/{app}", post(event_post))
}

#[debug_handler]
async fn robots_txt() -> (HeaderMap, &'static str) {
    let mut headers = HeaderMap::new();
    headers.append(
        header::CACHE_CONTROL,
        HeaderValue::from_static("public, max-age=2592000"), // 30 days
    );
    (
        headers,
        "User-agent: *
Disallow: /",
    )
}

#[debug_handler]
async fn event_options(headers: HeaderMap) -> HttpResult<HeaderMap> {
    let mut response_headers = HeaderMap::new();

    let origin = headers.get(header::ORIGIN).ok_or_else(|| {
        exception!(
            severity = Severity::Warn,
            code = error_code::FORDIDDEN,
            message = "access denied"
        )
    })?;
    response_headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, origin.clone());

    response_headers.insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static("POST, PUT, OPTIONS"),
    );
    response_headers.insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static("Accept, Content-Type"),
    );
    response_headers.insert(
        header::ACCESS_CONTROL_ALLOW_CREDENTIALS,
        HeaderValue::from_static("true"),
    );

    Ok(response_headers)
}

#[debug_handler]
async fn event_post(
    state: State<Arc<AppState>>,
    Path(app): Path<String>,
    headers: HeaderMap,
    Extension(client_info): Extension<Arc<ClientInfo>>,
    body: String,
) -> HttpResult<HeaderMap> {
    if !body.is_empty() {
        let request: SendEventRequest = json::from_json(&body)?;
        process_events(&state, &app, request, client_info).await?;
    }

    let origin = headers.get(header::ORIGIN);
    let mut response_headers = HeaderMap::new();
    if let Some(origin) = origin {
        response_headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, origin.clone());
        response_headers.insert(
            header::ACCESS_CONTROL_ALLOW_CREDENTIALS,
            HeaderValue::from_static("true"),
        );
    }
    Ok(response_headers)
}

async fn process_events(
    state: &Arc<AppState>,
    app: &str,
    request: SendEventRequest,
    client_info: Arc<ClientInfo>,
) -> HttpResult<()> {
    let now = Utc::now();
    for event in request.events {
        if let Err(error) = event.validate() {
            warn!("skip invalid event, error={error:?}");
            continue;
        }

        let mut message = EventMessage {
            id: log::id_generator::random_id(),
            date: event.date,
            app: app.to_string(),
            received_time: now,
            result: json::to_json_value(&event.result),
            action: event.action,
            error_code: event.error_code,
            error_message: event.error_message,
            elapsed: event.elapsed_time,
            context: event.context,
            stats: event.stats,
            info: event.info,
        };

        if let Some(ref user_agent) = client_info.user_agent {
            message.context.insert("user_agent".to_string(), user_agent.to_string());
        }

        message
            .context
            .insert("client_ip".to_string(), client_info.client_ip.to_string());

        state.producer.send(&state.topics.event, None, &message).await?;
    }

    Ok(())
}

#[derive(Deserialize, Debug)]
struct SendEventRequest {
    events: Vec<Event>,
}

#[derive(Deserialize, Debug)]
struct Event {
    date: DateTime<Utc>,
    result: EventResult,
    action: String,
    #[serde(rename = "errorCode")]
    error_code: Option<String>,
    #[serde(rename = "errorMessage")]
    error_message: Option<String>,
    context: HashMap<String, String>,
    stats: HashMap<String, f64>,
    info: HashMap<String, String>,
    #[serde(rename = "elapsedTime")]
    elapsed_time: i64,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum EventResult {
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

    fn validate(&self) -> CoreRsResult<()> {
        // Validate action for OK result
        if matches!(self.result, EventResult::Ok) && self.action.is_empty() {
            return Err(validation_error!(
                message = "action must not be empty if result is OK".to_string()
            ));
        }

        if (matches!(self.result, EventResult::Warn) || matches!(self.result, EventResult::Error))
            && self.error_code.as_ref().is_none_or(|s| s.is_empty())
        {
            return Err(validation_error!(
                message = "errorCode must not be empty if result is WARN/ERROR".to_string()
            ));
        }

        // Validate maps and estimate size
        let mut estimated_length = 0;
        estimated_length += Event::validate_map(&self.context, Event::MAX_KEY_LENGTH, Event::MAX_CONTEXT_VALUE_LENGTH)?;
        estimated_length += Event::validate_map(&self.info, Event::MAX_KEY_LENGTH, Event::MAX_INFO_VALUE_LENGTH)?;
        estimated_length += Event::validate_stats(&self.stats, Event::MAX_KEY_LENGTH)?;

        if estimated_length > Event::MAX_ESTIMATED_LENGTH {
            return Err(validation_error!(
                message = format!("event is too large, estimatedLength={estimated_length}")
            ));
        }

        Ok(())
    }

    fn validate_map(
        map: &HashMap<String, String>,
        max_key_length: usize,
        max_value_length: usize,
    ) -> CoreRsResult<usize> {
        let mut estimated_length = 0;
        for (key, value) in map {
            if key.len() > max_key_length {
                let truncated = Event::truncate(key, 50);
                return Err(validation_error!(
                    message = format!("key is too long, key={truncated}...(truncated)")
                ));
            }
            estimated_length += key.len();

            if value.len() > max_value_length {
                let truncated = Event::truncate(value, 200);
                return Err(validation_error!(
                    message = format!("value is too long, key={key}, value={truncated}...(truncated)")
                ));
            }
            estimated_length += value.len();
        }
        Ok(estimated_length)
    }

    fn validate_stats(stats: &HashMap<String, f64>, max_key_length: usize) -> CoreRsResult<usize> {
        let mut estimated_length = 0;
        for key in stats.keys() {
            if key.len() > max_key_length {
                let truncated = Event::truncate(key, 50);
                return Err(validation_error!(
                    message = format!("key is too long, key={truncated}...(truncated)")
                ));
            }
            estimated_length += key.len() + 5; // estimate double value as 5 chars
        }
        Ok(estimated_length)
    }

    fn truncate(s: &str, max_len: usize) -> &str {
        if s.len() <= max_len { s } else { &s[..max_len] }
    }
}
