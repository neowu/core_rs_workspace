use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::LineWriter;
use std::io::Write;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use framework::exception::CoreRsResult;
use framework::kafka::consumer::Message;
use rdkafka::message::ToBytes;
use serde::Deserialize;
use serde::Serialize;

use crate::AppState;
use crate::service::local_file_path;

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

pub async fn event_message_handler(state: Arc<AppState>, messages: Vec<Message<EventMessage>>) -> CoreRsResult<()> {
    let now = Utc::now().date_naive();
    let path = local_file_path("event", now, &state)?;

    let file = if path.exists() {
        OpenOptions::new().append(true).open(path)?
    } else {
        File::create(path)?
    };
    let mut writer = LineWriter::new(file);

    for message in messages {
        writer.write_all(message.payload.to_bytes())?;
        writer.write_all(b"\n")?;
    }
    writer.flush().unwrap();

    Ok(())
}
