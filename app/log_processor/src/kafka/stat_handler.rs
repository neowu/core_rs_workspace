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

// stat message schema from java core-ng framework
#[derive(Debug, Serialize, Deserialize)]
pub struct StatMessage {
    id: String,
    date: DateTime<Utc>,
    app: String,
    host: Option<String>,
    result: String,
    error_code: Option<String>,
    error_message: Option<String>,
    stats: Option<HashMap<String, f64>>,
    info: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize)]
pub struct StatDocument {
    #[serde(rename = "@timestamp")]
    timestamp: DateTime<Utc>,
    app: String,
    host: Option<String>,
    result: String,
    error_code: Option<String>,
    error_message: Option<String>,
    stats: Option<HashMap<String, f64>>,
    info: Option<HashMap<String, String>>,
}

pub async fn stat_message_handler(state: Arc<AppState>, messages: Vec<Message<StatMessage>>) -> Result<(), Exception> {
    let mut documents: Vec<(String, StatDocument)> = Vec::with_capacity(messages.len());
    for message in messages {
        let payload = message.payload()?;
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
    let now = Utc::now().date_naive();
    state.elasticsearch.bulk_index(&index(now), documents).await?;
    Ok(())
}

fn index(now: NaiveDate) -> String {
    format!("stat-{}", now.format("%Y.%m.%d")) // follow same pattern as elastic.co product line, e.g. metricbeats, in order to unify cleanup job
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;

    #[test]
    fn index() {
        assert_eq!(
            super::index(NaiveDate::from_ymd_opt(2025, 11, 5).unwrap()),
            "stat-2025.11.05"
        )
    }
}
