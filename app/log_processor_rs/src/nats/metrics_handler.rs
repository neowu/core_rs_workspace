use std::collections::HashMap;
use std::sync::Arc;

use framework::appender::MetricsMessage;
use framework::exception::Exception;
use framework_clickhouse::clickhouse;
use framework_clickhouse::clickhouse::Row;
use framework_clickhouse::types::DateTime64;
use framework_nats::consumer::Message;
use serde::Serialize;

use crate::AppState;
use crate::nats::Severity;

#[derive(Row, Serialize)]
struct MetricsRow {
    timestamp: DateTime64,
    id: String,
    app: String,
    host: String,
    severity: Severity,
    error_code: Option<String>,
    error_message: Option<String>,
    stats: HashMap<String, u64>,
    info: HashMap<String, String>,
}

pub(crate) async fn metrics_message_handler(
    state: Arc<AppState>,
    messages: Vec<Message<MetricsMessage>>,
) -> Result<(), Exception> {
    let mut rows = Vec::with_capacity(messages.len());
    for message in messages {
        let payload = message.payload;
        if let Some(service) = &state.alert_service {
            service.process_metrics(&payload);
        }
        rows.push(to_metrics_row(payload));
    }

    state.clickhouse.insert("metrics_rs", &rows).await?;
    Ok(())
}

fn to_metrics_row(payload: MetricsMessage) -> MetricsRow {
    MetricsRow {
        timestamp: payload.timestamp.into(),
        id: payload.id,
        app: payload.app,
        host: payload.host,
        severity: payload.severity.into(),
        error_code: payload.error_code,
        error_message: payload.error_message,
        stats: payload.stats.into_iter().collect(),
        info: payload.info.into_iter().collect(),
    }
}
