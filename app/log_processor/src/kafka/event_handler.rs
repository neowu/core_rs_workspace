use std::collections::HashMap;
use std::sync::Arc;

use chrono::DateTime;
use chrono::NaiveDate;
use chrono::Utc;
use framework::exception::Exception;
use framework::kafka::consumer::Message;
use serde::Deserialize;
use serde::Serialize;

use crate::AppState;

// event message schema from java core-ng framework
#[derive(Debug, Serialize, Deserialize)]
pub struct EventMessage {
    id: String,
    date: DateTime<Utc>,
    app: String,
    received_time: DateTime<Utc>,
    result: String,
    action: String,
    error_code: Option<String>,
    error_message: Option<String>,
    elapsed: i64,
    context: HashMap<String, String>,
    stats: Option<HashMap<String, f64>>,
    info: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize)]
pub struct EventDocument {
    #[serde(rename = "@timestamp")]
    timestamp: DateTime<Utc>,
    app: String,
    received_time: DateTime<Utc>,
    result: String,
    action: String,
    error_code: Option<String>,
    error_message: Option<String>,
    context: HashMap<String, String>,
    stats: Option<HashMap<String, f64>>,
    info: Option<HashMap<String, String>>,
    elapsed: i64,
}

pub async fn event_message_handler(state: Arc<AppState>, messages: Vec<Message<EventMessage>>) -> Result<(), Exception> {
    let mut documents: Vec<(String, EventDocument)> = Vec::with_capacity(messages.len());
    for message in messages {
        let payload = message.payload()?;
        let doc = EventDocument {
            timestamp: payload.date,
            app: payload.app,
            received_time: payload.received_time,
            result: payload.result,
            action: payload.action,
            error_code: payload.error_code,
            error_message: payload.error_message,
            context: payload.context,
            stats: payload.stats,
            info: payload.info,
            elapsed: payload.elapsed,
        };
        documents.push((payload.id, doc));
    }
    let now = Utc::now().date_naive();
    state.elasticsearch.bulk_index(&index(now), documents).await?;
    Ok(())
}

fn index(now: NaiveDate) -> String {
    format!("event-{}", now.format("%Y.%m.%d"))
}
