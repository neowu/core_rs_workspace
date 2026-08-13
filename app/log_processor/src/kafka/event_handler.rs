use std::collections::HashMap;
use std::sync::Arc;

use framework::date::Date;
use framework::date::DateTime;
use framework::exception::Exception;
use framework_clickhouse::ClickHouse;
use framework_clickhouse::Enum8;
use framework_clickhouse::clickhouse;
use framework_clickhouse::clickhouse::Row;
use framework_clickhouse::data_type::DateTime64;
use framework_clickhouse::data_type::Decimal64;
use framework_kafka::consumer::Message;
use serde::Deserialize;
use serde::Serialize;

use crate::AppState;
use crate::elasticsearch::Elasticsearch;

// event message schema from java core-ng framework
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct EventMessage {
    id: String,
    timestamp: DateTime, // server received_time
    app: String,
    client_timestamp: DateTime,
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
struct EventDocument {
    #[serde(rename = "@timestamp")]
    timestamp: DateTime,
    app: String,
    client_timestamp: DateTime,
    result: String,
    action: String,
    error_code: Option<String>,
    error_message: Option<String>,
    context: HashMap<String, String>,
    stats: Option<HashMap<String, f64>>,
    info: Option<HashMap<String, String>>,
    elapsed: i64,
}

pub(crate) async fn event_message_handler(
    state: Arc<AppState>,
    messages: Vec<Message<EventMessage>>,
) -> Result<(), Exception> {
    if let Some(clickhouse) = &state.clickhouse {
        insert_to_clickhouse(clickhouse, &messages).await?;
    }

    index_to_elasticsearch(&state.elasticsearch, messages).await?;
    Ok(())
}

async fn index_to_elasticsearch(
    elasticsearch: &Elasticsearch,
    messages: Vec<Message<EventMessage>>,
) -> Result<(), Exception> {
    let mut documents: Vec<(String, EventDocument)> = Vec::with_capacity(messages.len());
    for message in messages {
        let payload = message.payload;
        let doc = EventDocument {
            timestamp: payload.timestamp,
            app: payload.app,
            client_timestamp: payload.client_timestamp,
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
    let now = DateTime::now().date();
    elasticsearch.bulk_index(&index(now), documents).await?;
    Ok(())
}

fn index(now: Date) -> String {
    let (year, month, day) = now.to_ymd();
    format!("event-{year}.{month:02}.{day:02}")
}

#[derive(Row, Serialize)]
struct EventRow {
    pub timestamp: DateTime64,
    pub id: String,
    pub app: String,
    pub client_timestamp: DateTime64,
    pub result: EventResult,
    pub action: String,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub context: HashMap<String, String>,
    pub stats: HashMap<String, Decimal64<3>>,
    pub info: HashMap<String, String>,
}

// Enum8('OK' = 1, 'WARN' = 2, 'ERROR' = 3)
#[derive(Enum8)]
enum EventResult {
    Ok = 1,
    Warn = 2,
    Error = 3,
}

async fn insert_to_clickhouse(clickhouse: &ClickHouse, messages: &[Message<EventMessage>]) -> Result<(), Exception> {
    let events: Vec<EventRow> = messages.iter().map(|message| to_event_row(&message.payload)).collect();
    clickhouse.insert("event", &events).await
}

fn to_event_row(payload: &EventMessage) -> EventRow {
    // elapsed is flattened into the numeric stats map, same as the action table
    let mut stats: HashMap<String, Decimal64<3>> =
        payload.stats.iter().flatten().map(|(key, value)| (key.clone(), Decimal64::from(*value))).collect();
    stats.insert("elapsed".to_owned(), Decimal64::from(payload.elapsed as f64));

    EventRow {
        timestamp: payload.timestamp.into(),
        id: payload.id.clone(),
        app: payload.app.clone(),
        client_timestamp: payload.client_timestamp.into(),
        result: to_event_result(&payload.result),
        action: payload.action.clone(),
        error_code: payload.error_code.clone(),
        error_message: payload.error_message.clone(),
        context: payload.context.clone(),
        stats,
        info: payload.info.clone().unwrap_or_default(),
    }
}

fn to_event_result(result: &str) -> EventResult {
    match result {
        "WARN" => EventResult::Warn,
        "ERROR" => EventResult::Error,
        _ => EventResult::Ok,
    }
}
