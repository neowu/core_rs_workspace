use std::collections::HashMap;
use std::sync::Arc;

use framework::time::Date;
use framework::time::DateTime;
use framework::exception::Exception;
use framework_clickhouse::ClickHouse;
use framework_clickhouse::Enum8;
use framework_clickhouse::clickhouse;
use framework_clickhouse::clickhouse::Row;
use framework_clickhouse::types::DateTime64;
use framework_clickhouse::types::Decimal64;
use framework_kafka::consumer::Message;
use serde::Deserialize;
use serde::Serialize;

use crate::AppState;
use crate::elasticsearch::Elasticsearch;

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
struct StatDocument {
    #[serde(rename = "@timestamp")]
    timestamp: DateTime,
    app: String,
    host: Option<String>,
    result: String,
    error_code: Option<String>,
    error_message: Option<String>,
    stats: Option<HashMap<String, f64>>,
    info: Option<HashMap<String, String>>,
}

pub(crate) async fn stat_message_handler(
    state: Arc<AppState>,
    messages: Vec<Message<StatMessage>>,
) -> Result<(), Exception> {
    if let Some(clickhouse) = &state.clickhouse {
        insert_to_clickhouse(clickhouse, &messages).await?;
    }

    index_to_elasticsearch(&state.elasticsearch, messages).await?;
    Ok(())
}

async fn index_to_elasticsearch(
    elasticsearch: &Elasticsearch,
    messages: Vec<Message<StatMessage>>,
) -> Result<(), Exception> {
    let mut documents: Vec<(String, StatDocument)> = Vec::with_capacity(messages.len());
    for message in messages {
        let payload = message.payload;
        let doc = StatDocument {
            timestamp: payload.date,
            app: payload.app,
            host: payload.host,
            result: payload.result,
            error_code: payload.error_code,
            error_message: payload.error_message,
            stats: payload.stats,
            info: payload.info,
        };
        documents.push((payload.id, doc));
    }
    let now = DateTime::now().date();
    elasticsearch.bulk_index(&index(now), documents).await?;
    Ok(())
}

fn index(now: Date) -> String {
    let (year, month, day) = now.to_ymd();
    format!("stat-{year}.{month:02}.{day:02}") // follow same pattern as elastic.co product line, e.g. metricbeats, in order to unify cleanup job
}

#[derive(Row, Serialize)]
struct StatRow {
    pub timestamp: DateTime64,
    pub id: String,
    pub app: String,
    pub host: String,
    pub result: StatResult,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub stats: HashMap<String, Decimal64<3>>,
    pub info: HashMap<String, String>,
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
    clickhouse.insert("stat", &stats).await
}

fn to_stat_row(payload: &StatMessage) -> StatRow {
    let stats: HashMap<String, Decimal64<3>> =
        payload.stats.iter().flatten().map(|(key, value)| (key.clone(), Decimal64::from(*value))).collect();

    StatRow {
        timestamp: payload.date.into(),
        id: payload.id.clone(),
        app: payload.app.clone(),
        host: payload.host.clone().unwrap_or_default(),
        result: to_stat_result(&payload.result),
        error_code: payload.error_code.clone(),
        error_message: payload.error_message.clone(),
        stats,
        info: payload.info.clone().unwrap_or_default(),
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
    use framework::time::Date;

    #[test]
    fn index() {
        assert_eq!(super::index(Date::new(2025, 11, 5)), "stat-2025.11.05");
    }
}
