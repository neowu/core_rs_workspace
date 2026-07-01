use std::collections::HashMap;
use std::sync::Arc;

use chrono::DateTime;
use chrono::NaiveDate;
use chrono::Utc;
use framework::exception::Exception;
use framework_kafka::consumer::Message;
use serde::Deserialize;
use serde::Serialize;
use time::Duration;
use time::OffsetDateTime;

use crate::AppState;
use crate::clickhouse::ActionResult;
use crate::clickhouse::ActionRow;
use crate::clickhouse::TraceRow;

// action log message schema from java core-ng framework
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ActionLogMessage {
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
struct PerformanceStatMessage {
    total_elapsed: i64,
    count: i64,
    read_entries: Option<i64>,
    write_entries: Option<i64>,
    read_bytes: Option<i64>,
    write_bytes: Option<i64>,
}

#[derive(Debug, Serialize)]
struct ActionLogDocument {
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
struct TraceDocument {
    #[serde(rename = "@timestamp")]
    timestamp: DateTime<Utc>,
    app: String,
    result: String,
    action: String,
    error_code: Option<String>,
    content: String,
}

pub(crate) async fn action_log_message_handler(
    state: Arc<AppState>,
    messages: Vec<Message<ActionLogMessage>>,
) -> Result<(), Exception> {
    insert_to_clickhouse(&state, &messages).await?;
    index_to_elasticsearch(&state, messages).await?;
    Ok(())
}

async fn index_to_elasticsearch(
    state: &Arc<AppState>,
    messages: Vec<Message<ActionLogMessage>>,
) -> Result<(), Exception> {
    let mut documents: Vec<(String, ActionLogDocument)> = Vec::with_capacity(messages.len());
    let mut traces: Vec<(String, TraceDocument)> = vec![];
    for message in messages {
        let payload = message.payload;
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
            let trace_doc = TraceDocument {
                timestamp: payload.date,
                app: payload.app,
                result: payload.result,
                action: payload.action,
                error_code: payload.error_code,
                content,
            };
            traces.push((payload.id, trace_doc));
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

async fn insert_to_clickhouse(state: &Arc<AppState>, messages: &[Message<ActionLogMessage>]) -> Result<(), Exception> {
    let mut actions = Vec::with_capacity(messages.len());
    let mut traces = vec![];
    for message in messages {
        let payload = &message.payload;
        actions.push(to_action_row(payload));
        if let Some(content) = &payload.trace_log {
            let trace = TraceRow {
                timestamp: to_offset_datetime(message.payload.date),
                id: payload.id.clone(),
                content: content.clone(),
                app: payload.app.clone(),
                error_code: payload.error_code.clone(),
            };
            traces.push(trace);
        }
    }

    state.clickhouse.insert("action", &actions).await?;
    if !traces.is_empty() {
        state.clickhouse.insert("trace", &traces).await?;
    }
    Ok(())
}

fn to_action_row(payload: &ActionLogMessage) -> ActionRow {
    // a single value goes into context; multiple values go into multi_context. None becomes "".
    let mut context: HashMap<String, String> = HashMap::new();
    let mut multi_context: HashMap<String, Vec<String>> = HashMap::new();
    for (key, values) in &payload.context {
        if values.len() == 1
            && let Some(Some(first)) = values.first()
        {
            context.insert(key.to_owned(), first.to_owned());
        } else {
            let values: Vec<String> = values.iter().flatten().map(String::to_owned).collect();
            multi_context.insert(key.to_owned(), values);
        }
    }

    if let Some(clients) = &payload.clients {
        if clients.len() == 1
            && let Some(first) = clients.first()
        {
            context.insert("client".to_owned(), first.to_owned());
        } else {
            multi_context.insert("client".to_owned(), clients.clone());
        }
    }

    // a single ref_id goes into ref_id; multiple ref_ids go into ref_ids.
    let (ref_id, ref_ids) = match &payload.ref_ids {
        Some(ids) if ids.len() == 1 => (ids.first().map(String::to_owned).clone(), Vec::new()),
        Some(ids) => (None, ids.clone()),
        None => (None, Vec::new()),
    };

    // elapsed and every perf_stat counter are flattened into the numeric stats map; f64 stats are rounded to i64.
    let mut stats: HashMap<String, i64> =
        payload.stats.iter().flatten().map(|(key, value)| (key.clone(), value.round() as i64)).collect();
    stats.insert("elapsed".to_owned(), payload.elapsed);
    if let Some(perf_stats) = &payload.perf_stats {
        for (key, perf) in perf_stats {
            stats.insert(format!("{key}_elapsed"), perf.total_elapsed);
            stats.insert(format!("{key}_count"), perf.count);
            if let Some(value) = perf.read_entries {
                stats.insert(format!("{key}_read_entries"), value);
            }
            if let Some(value) = perf.write_entries {
                stats.insert(format!("{key}_write_entries"), value);
            }
            if let Some(value) = perf.read_bytes {
                stats.insert(format!("{key}_read_bytes"), value);
            }
            if let Some(value) = perf.write_bytes {
                stats.insert(format!("{key}_write_bytes"), value);
            }
        }
    }

    ActionRow {
        timestamp: to_offset_datetime(payload.date),
        id: payload.id.clone(),
        app: payload.app.clone(),
        host: payload.host.clone(),
        result: to_action_result(&payload.result),
        action: payload.action.clone(),
        ref_id,
        ref_ids,
        error_code: payload.error_code.clone(),
        error_message: payload.error_message.clone(),
        context,
        multi_context,
        stats,
    }
}

fn to_action_result(result: &str) -> ActionResult {
    match result {
        "WARN" => ActionResult::Warn,
        "ERROR" => ActionResult::Error,
        _ => ActionResult::Ok,
    }
}

fn to_offset_datetime(date: DateTime<Utc>) -> OffsetDateTime {
    OffsetDateTime::UNIX_EPOCH + Duration::milliseconds(date.timestamp_millis())
}
