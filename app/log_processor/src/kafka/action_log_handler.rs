use std::collections::HashMap;
use std::sync::Arc;

use chrono::DateTime;
use chrono::NaiveDate;
use chrono::Utc;
use framework::exception::Exception;
use framework_clickhouse::Serialize_repr;
use framework_clickhouse::clickhouse;
use framework_macro::Row;
use framework_kafka::consumer::Message;
use serde::Deserialize;
use serde::Serialize;

use crate::AppState;

// action log message schema from java core-ng framework
#[derive(Debug, Deserialize)]
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

pub(crate) async fn action_log_message_handler(
    state: Arc<AppState>,
    messages: Vec<Message<ActionLogMessage>>,
) -> Result<(), Exception> {
    insert_to_clickhouse(&state, &messages).await?;
    index_to_elasticsearch(&state, messages).await?;
    Ok(())
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

#[derive(Row, Serialize)]
#[table(name = "action")]
struct ActionRow {
    // DateTime64(3, 'UTC') is encoded as i64 milliseconds; the chrono feature provides this serde helper.
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    #[column(name = "timestamp")]
    pub timestamp: DateTime<Utc>,
    #[column(name = "id")]
    pub id: String,
    #[column(name = "app")]
    pub app: String,
    #[column(name = "host")]
    pub host: String,
    #[column(name = "result")]
    pub result: ActionResult,
    #[column(name = "action")]
    pub action: String,
    #[column(name = "ref_id")]
    pub ref_id: Option<String>,
    #[column(name = "ref_ids")]
    pub ref_ids: Vec<String>,
    #[column(name = "error_code")]
    pub error_code: Option<String>,
    #[column(name = "error_message")]
    pub error_message: Option<String>,
    #[column(name = "context")]
    pub context: HashMap<String, String>,
    #[column(name = "multi_context")]
    pub multi_context: HashMap<String, Vec<String>>,
    #[column(name = "stats")]
    pub stats: HashMap<String, i64>,
}

// Enum8('OK' = 1, 'WARN' = 2, 'ERROR' = 3); serialized as its i8 discriminant.
#[derive(Serialize_repr)]
#[repr(i8)]
enum ActionResult {
    Ok = 1,
    Warn = 2,
    Error = 3,
}

#[derive(Row, Serialize)]
#[table(name = "trace")]
struct TraceRow {
    // DateTime64(3, 'UTC') is encoded as i64 milliseconds; the chrono feature provides this serde helper.
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    #[column(name = "timestamp")]
    pub timestamp: DateTime<Utc>,
    #[column(name = "id")]
    pub id: String,
    #[column(name = "app")]
    pub app: String,
    #[column(name = "error_code")]
    pub error_code: Option<String>,
    #[column(name = "content")]
    pub content: String,
}

async fn insert_to_clickhouse(state: &Arc<AppState>, messages: &[Message<ActionLogMessage>]) -> Result<(), Exception> {
    let mut actions = Vec::with_capacity(messages.len());
    let mut traces = vec![];
    for message in messages {
        let payload = &message.payload;
        actions.push(to_action_row(payload));
        if let Some(content) = &payload.trace_log {
            let trace = TraceRow {
                timestamp: message.payload.date,
                id: payload.id.clone(),
                content: content.clone(),
                app: payload.app.clone(),
                error_code: payload.error_code.clone(),
            };
            traces.push(trace);
        }
    }

    state.clickhouse.insert(&actions).await?;
    if !traces.is_empty() {
        state.clickhouse.insert(&traces).await?;
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
        timestamp: payload.date,
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
