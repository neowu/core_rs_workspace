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
    stats: Option<HashMap<String, f64>>,
    perf_stats: Option<HashMap<String, PerformanceStatMessage>>,
    trace_log: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PerformanceStatMessage {
    total_elapsed: i64,
    count: i64,
    read_entries: Option<i64>,
    write_entries: Option<i64>,
    read_bytes: Option<i64>,
    write_bytes: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct ActionLogDocument {
    #[serde(rename = "@timestamp")]
    timestamp: DateTime<Utc>,
    app: String,
    host: String,
    result: String,
    action: String,
    #[serde(rename = "correlation_id")]
    correlation_ids: Option<Vec<String>>,
    #[serde(rename = "client")]
    clients: Option<Vec<String>>,
    #[serde(rename = "ref_id")]
    ref_ids: Option<Vec<String>>,
    error_code: Option<String>,
    error_message: Option<String>,
    elapsed: i64,
    context: HashMap<String, Vec<Option<String>>>,
    stats: Option<HashMap<String, f64>>,
    perf_stats: Option<HashMap<String, PerformanceStatMessage>>,
}

#[derive(Debug, Serialize)]
pub struct TraceDocument {
    #[serde(rename = "@timestamp")]
    timestamp: DateTime<Utc>,
    app: String,
    result: String,
    action: String,
    error_code: Option<String>,
    content: String,
}

pub async fn action_log_message_handler(
    state: Arc<AppState>,
    messages: Vec<Message<ActionLogMessage>>,
) -> Result<(), Exception> {
    let mut documents: Vec<(String, ActionLogDocument)> = Vec::with_capacity(messages.len());
    let mut traces: Vec<(String, TraceDocument)> = vec![];
    for message in messages {
        let payload = message.payload()?;
        let doc = ActionLogDocument {
            timestamp: payload.date,
            app: payload.app.clone(),
            host: payload.host,
            result: payload.result.clone(),
            action: payload.action.clone(),
            correlation_ids: payload.correlation_ids,
            clients: payload.clients,
            ref_ids: payload.ref_ids,
            error_code: payload.error_code.clone(),
            error_message: payload.error_message,
            elapsed: payload.elapsed,
            context: payload.context,
            stats: payload.stats,
            perf_stats: payload.perf_stats,
        };
        documents.push((payload.id.clone(), doc));

        if let Some(content) = payload.trace_log {
            let doc = TraceDocument {
                timestamp: payload.date,
                app: payload.app,
                result: payload.result,
                action: payload.action,
                error_code: payload.error_code,
                content,
            };
            traces.push((payload.id, doc));
        }
    }
    let now = Utc::now().date_naive();
    state.elasticsearch.bulk_index(&action_index(now), documents).await?;
    if !traces.is_empty() {
        state.elasticsearch.bulk_index(&trace_index(now), traces).await?;
    }
    Ok(())
}

fn action_index(now: NaiveDate) -> String {
    format!("action-{}", now.format("%Y.%m.%d"))
}

fn trace_index(now: NaiveDate) -> String {
    format!("trace-{}", now.format("%Y.%m.%d"))
}
