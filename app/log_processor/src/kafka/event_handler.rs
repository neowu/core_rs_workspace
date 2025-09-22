use std::collections::HashMap;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use framework::exception::CoreRsResult;
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
    stats: HashMap<String, f64>,
    info: HashMap<String, String>,
}

pub async fn event_message_handler(_state: Arc<AppState>, _messages: Vec<Message<EventMessage>>) -> CoreRsResult<()> {
    Ok(())
}
