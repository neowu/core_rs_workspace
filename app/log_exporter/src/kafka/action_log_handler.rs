use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::LineWriter;
use std::io::Write;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use framework::exception::CoreRsResult;
use framework::json;
use framework::kafka::consumer::Message;
use rdkafka::message::ToBytes;
use serde::Deserialize;
use serde::Serialize;

use crate::AppState;
use crate::service::local_file_path;

// action log message schema from java core-ng framework
#[derive(Debug, Serialize, Deserialize)]
pub struct ActionLogMessage {
    id: String,
    date: DateTime<Utc>,
    app: String,
    host: String,
    result: String,
    action: String,
    correlation_ids: Option<Vec<String>>,
    clients: Option<Vec<String>>,
    ref_ids: Option<Vec<String>>,
    error_code: Option<String>,
    error_message: Option<String>,
    elapsed: i64,
    context: HashMap<String, Vec<Option<String>>>,
    stats: HashMap<String, f64>,
    perf_stats: HashMap<String, PerformanceStatMessage>,
    // trace_log: Option<String>,   strip trace_log
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PerformanceStatMessage {
    total_elapsed: i64,
    count: i64,
    read_entries: Option<i64>,
    write_entries: Option<i64>,
}

pub async fn action_log_message_handler(
    state: Arc<AppState>,
    messages: Vec<Message<ActionLogMessage>>,
) -> CoreRsResult<()> {
    let now = Utc::now().date_naive();
    let path = local_file_path("action", now, &state)?;

    let file = if path.exists() {
        OpenOptions::new().append(true).open(path)?
    } else {
        File::create(path)?
    };
    let mut writer = LineWriter::new(file);

    for message in messages {
        let log = message.payload()?;
        writer.write_all(json::to_json(&log)?.to_bytes())?;
        writer.write_all(b"\n")?;
    }
    writer.flush().unwrap();

    Ok(())
}
