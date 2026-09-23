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

// stat message schema from java core-ng framework
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct StatMessage {
    id: String,
    date: DateTime,
    app: String,
    host: Option<String>,
    result: String,
    error_code: Option<String>,
    error_message: Option<String>,
    stats: Option<HashMap<String, f64>>,
    info: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize)]
struct StatDocument<'a> {
    #[serde(rename = "@timestamp")]
    timestamp: DateTime,
    app: &'a str,
    host: Option<&'a str>,
    result: &'a str,
    error_code: Option<&'a str>,
    error_message: Option<&'a str>,
    stats: Option<&'a HashMap<String, f64>>,
    info: Option<&'a HashMap<String, String>>,
}

pub(crate) async fn stat_message_handler(
    state: Arc<AppState>,
    messages: Vec<Message<StatMessage>>,
) -> Result<(), Exception> {
    if let Some(clickhouse) = &state.clickhouse {
        insert_to_clickhouse(clickhouse, &messages).await?;
    }

    index_to_elasticsearch(&state.elasticsearch, &messages).await?;
    Ok(())
}

async fn index_to_elasticsearch(
    elasticsearch: &Elasticsearch,
    messages: &[Message<StatMessage>],
) -> Result<(), Exception> {
    let documents = messages.iter().map(|message| {
        let payload = &message.payload;
        (payload.id.as_str(), to_stat_document(payload))
    });
    let now = DateTime::now().date();
    elasticsearch.bulk_index(&index(now), documents).await?;
    Ok(())
}

fn to_stat_document(payload: &StatMessage) -> StatDocument<'_> {
    StatDocument {
        timestamp: payload.date,
        app: &payload.app,
        host: payload.host.as_deref(),
        result: &payload.result,
        error_code: payload.error_code.as_deref(),
        error_message: payload.error_message.as_deref(),
        stats: payload.stats.as_ref(),
        info: payload.info.as_ref(),
    }
}

fn index(now: Date) -> String {
    let (year, month, day) = now.to_ymd();
    format!("stat-{year}.{month:02}.{day:02}") // follow same pattern as elastic.co product line, e.g. metricbeats, in order to unify cleanup job
}

#[derive(Row, Serialize)]
struct StatRow<'a> {
    pub timestamp: DateTime64,
    pub id: &'a str,
    pub app: &'a str,
    pub host: &'a str,
    pub result: StatResult,
    pub error_code: Option<&'a str>,
    pub error_message: Option<&'a str>,
    pub stats: Stats<'a>,
    pub info: OptionMap<'a, String>,
}

// Enum8('OK' = 1, 'WARN' = 2, 'ERROR' = 3)
#[derive(Enum8)]
enum StatResult {
    Ok = 1,
    Warn = 2,
    Error = 3,
}

async fn insert_to_clickhouse(clickhouse: &ClickHouse, messages: &[Message<StatMessage>]) -> Result<(), Exception> {
    let stats: Vec<StatRow> = messages.iter().map(|message| to_stat_row(&message.payload)).collect();
    clickhouse.insert_borrowed::<StatRow>("stat", &stats).await
}

fn to_stat_row(payload: &StatMessage) -> StatRow<'_> {
    StatRow {
        timestamp: payload.date.into(),
        id: &payload.id,
        app: &payload.app,
        host: payload.host.as_deref().unwrap_or_default(),
        result: to_stat_result(&payload.result),
        error_code: payload.error_code.as_deref(),
        error_message: payload.error_message.as_deref(),
        // the stat table has no elapsed of its own, the message is already a bag of numbers
        stats: Stats { elapsed: None, stats: payload.stats.as_ref() },
        info: OptionMap(payload.info.as_ref()),
    }
}

fn to_stat_result(result: &str) -> StatResult {
    match result {
        "WARN" => StatResult::Warn,
        "ERROR" => StatResult::Error,
        _ => StatResult::Ok,
    }
}

#[cfg(test)]
mod tests {
    use framework::json;
    use framework::time::Date;

    #[test]
    fn borrowed_document_preserves_json() {
        let payload = json::from_json::<super::StatMessage>(
            r#"{"id":"s1","date":"2026-08-12T01:02:03Z","app":"app","result":"OK","stats":{},"info":{"key":"value"}}"#,
        )
        .expect("valid stat message");
        assert_eq!(
            json::to_json(&super::to_stat_document(&payload)).expect("serialize stat"),
            concat!(
                r#"{"@timestamp":"2026-08-12T01:02:03Z","app":"app","host":null,"result":"OK","#,
                r#""error_code":null,"error_message":null,"stats":{},"info":{"key":"value"}}"#,
            ),
        );
    }

    #[test]
    fn index() {
        assert_eq!(super::index(Date::new(2025, 11, 5)), "stat-2025.11.05");
    }
}
