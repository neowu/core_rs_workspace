use std::collections::HashMap;
use std::sync::Arc;

use framework::exception::Exception;
use framework::time::Date;
use framework::time::DateTime;
use framework_clickhouse::ClickHouse;
use framework_clickhouse::Enum8;
use framework_clickhouse::clickhouse;
use framework_clickhouse::clickhouse::Row;
use framework_clickhouse::types::DateTime64;
use framework_kafka::consumer::Message;
use serde::Deserialize;
use serde::Serialize;

use crate::AppState;
use crate::elasticsearch::Elasticsearch;
use crate::kafka::OptionMap;
use crate::kafka::Stats;

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
struct EventDocument<'a> {
    #[serde(rename = "@timestamp")]
    timestamp: DateTime,
    app: &'a str,
    client_timestamp: DateTime,
    result: &'a str,
    action: &'a str,
    error_code: Option<&'a str>,
    error_message: Option<&'a str>,
    context: &'a HashMap<String, String>,
    stats: Option<&'a HashMap<String, f64>>,
    info: Option<&'a HashMap<String, String>>,
    elapsed: i64,
}

pub(crate) async fn event_message_handler(
    state: Arc<AppState>,
    messages: Vec<Message<EventMessage>>,
) -> Result<(), Exception> {
    if let Some(clickhouse) = &state.clickhouse {
        insert_to_clickhouse(clickhouse, &messages).await?;
    }

    index_to_elasticsearch(&state.elasticsearch, &messages).await?;
    Ok(())
}

async fn index_to_elasticsearch(
    elasticsearch: &Elasticsearch,
    messages: &[Message<EventMessage>],
) -> Result<(), Exception> {
    let documents = messages.iter().map(|message| {
        let payload = &message.payload;
        (payload.id.as_str(), to_event_document(payload))
    });
    let now = DateTime::now().date();
    elasticsearch.bulk_index(&index(now), documents).await?;
    Ok(())
}

fn to_event_document(payload: &EventMessage) -> EventDocument<'_> {
    EventDocument {
        timestamp: payload.timestamp,
        app: &payload.app,
        client_timestamp: payload.client_timestamp,
        result: &payload.result,
        action: &payload.action,
        error_code: payload.error_code.as_deref(),
        error_message: payload.error_message.as_deref(),
        context: &payload.context,
        stats: payload.stats.as_ref(),
        info: payload.info.as_ref(),
        elapsed: payload.elapsed,
    }
}

fn index(now: Date) -> String {
    let (year, month, day) = now.to_ymd();
    format!("event-{year}.{month:02}.{day:02}")
}

#[derive(Row, Serialize)]
struct EventRow<'a> {
    pub timestamp: DateTime64,
    pub id: &'a str,
    pub app: &'a str,
    pub client_timestamp: DateTime64,
    pub result: EventResult,
    pub action: &'a str,
    pub error_code: Option<&'a str>,
    pub error_message: Option<&'a str>,
    pub context: &'a HashMap<String, String>,
    pub stats: Stats<'a>,
    pub info: OptionMap<'a, String>,
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
    clickhouse.insert_borrowed::<EventRow>("event", &events).await
}

fn to_event_row(payload: &EventMessage) -> EventRow<'_> {
    EventRow {
        timestamp: payload.timestamp.into(),
        id: &payload.id,
        app: &payload.app,
        client_timestamp: payload.client_timestamp.into(),
        result: to_event_result(&payload.result),
        action: &payload.action,
        error_code: payload.error_code.as_deref(),
        error_message: payload.error_message.as_deref(),
        context: &payload.context,
        // elapsed is flattened into the numeric stats map, same as the action table
        stats: Stats { elapsed: Some(payload.elapsed), stats: payload.stats.as_ref() },
        info: OptionMap(payload.info.as_ref()),
    }
}

fn to_event_result(result: &str) -> EventResult {
    match result {
        "WARN" => EventResult::Warn,
        "ERROR" => EventResult::Error,
        _ => EventResult::Ok,
    }
}

#[cfg(test)]
mod tests {
    use framework::json;

    #[test]
    fn borrowed_document_preserves_json() {
        let payload = json::from_json::<super::EventMessage>(
            r#"{"id":"e1","timestamp":"2026-08-12T01:02:03Z","app":"app",
                "client_timestamp":"2026-08-12T01:02:02Z","result":"OK","action":"test",
                "elapsed":12,"context":{"key":"value"},"info":{}}"#,
        )
        .expect("valid event message");
        assert_eq!(
            json::to_json(&super::to_event_document(&payload)).expect("serialize event"),
            concat!(
                r#"{"@timestamp":"2026-08-12T01:02:03Z","app":"app","client_timestamp":"2026-08-12T01:02:02Z","#,
                r#""result":"OK","action":"test","error_code":null,"error_message":null,"#,
                r#""context":{"key":"value"},"stats":null,"info":{},"elapsed":12}"#,
            ),
        );
    }
}
