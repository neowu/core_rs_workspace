use std::collections::HashMap;
use std::sync::Arc;

use framework::date::Date;
use framework::date::DateTime;
use framework::exception::Exception;
use framework_kafka::consumer::Message;
use serde::Deserialize;
use serde::Serialize;

use crate::AppState;

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
    state.elasticsearch.bulk_index(&index(now), documents).await?;
    Ok(())
}

fn index(now: Date) -> String {
    let (year, month, day) = now.to_ymd();
    format!("stat-{year}.{month:02}.{day:02}") // follow same pattern as elastic.co product line, e.g. metricbeats, in order to unify cleanup job
}

#[cfg(test)]
mod tests {
    use framework::date::Date;

    #[test]
    fn index() {
        assert_eq!(super::index(Date::new(2025, 11, 5)), "stat-2025.11.05");
    }
}
