use std::sync::Arc;

use framework::appender::MetricsMessage;
use framework::exception::Exception;
use framework_clickhouse::clickhouse;
use framework_clickhouse::clickhouse::Row;
use framework_clickhouse::types::DateTime64;
use framework_clickhouse::types::Map;
use framework_nats::consumer::Message;
use serde::Serialize;

use crate::AppState;
use crate::nats::Severity;

// borrows from the message, same as ActionRow
#[derive(Row, Serialize)]
struct MetricsRow<'a> {
    timestamp: DateTime64,
    id: &'a str,
    app: &'a str,
    host: &'a str,
    severity: Severity,
    error_code: Option<&'a str>,
    error_message: Option<&'a str>,
    stats: Map<'a, String, u64>,
    info: Map<'a, String, String>,
}

pub(crate) async fn metrics_message_handler(
    state: Arc<AppState>,
    messages: Vec<Message<MetricsMessage>>,
) -> Result<(), Exception> {
    let mut rows = Vec::with_capacity(messages.len());
    for message in &messages {
        let payload = &message.payload;
        if let Some(service) = &state.alert_service {
            service.process_metrics(payload);
        }
        rows.push(to_metrics_row(payload));
    }

    state.clickhouse.insert_borrowed::<MetricsRow>("metrics_rs", &rows).await?;
    Ok(())
}

fn to_metrics_row(payload: &MetricsMessage) -> MetricsRow<'_> {
    MetricsRow {
        timestamp: payload.timestamp.into(),
        id: &payload.id,
        app: &payload.app,
        host: &payload.host,
        severity: payload.severity.into(),
        error_code: payload.error_code.as_deref(),
        error_message: payload.error_message.as_deref(),
        stats: Map(&payload.stats),
        info: Map(&payload.info),
    }
}
