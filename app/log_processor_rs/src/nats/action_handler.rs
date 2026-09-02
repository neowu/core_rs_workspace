use std::collections::HashMap;
use std::sync::Arc;

use framework::appender::ActionMessage;
use framework::exception::Exception;
use framework_clickhouse::clickhouse;
use framework_clickhouse::clickhouse::Row;
use framework_clickhouse::types::DateTime64;
use framework_nats::consumer::Message;
use serde::Serialize;

use crate::AppState;
use crate::nats::Severity;

#[derive(Row, Serialize)]
struct ActionRow {
    timestamp: DateTime64,
    id: String,
    app: String,
    host: String,
    severity: Severity,
    kind: String,
    ref_id: Option<String>,
    ref_ids: Vec<String>,
    error_code: Option<String>,
    error_message: Option<String>,
    context: HashMap<String, String>,
    multi_context: HashMap<String, Vec<String>>,
    stats: HashMap<String, u64>,
}

#[derive(Row, Serialize)]
struct TraceRow {
    timestamp: DateTime64,
    id: String,
    app: String,
    error_code: Option<String>,
    content: String,
}

pub(crate) async fn action_message_handler(
    state: Arc<AppState>,
    messages: Vec<Message<ActionMessage>>,
) -> Result<(), Exception> {
    let mut actions = Vec::with_capacity(messages.len());
    let mut traces = vec![];
    for message in messages {
        let mut payload = message.payload;
        // logs are only carried by traced or failed actions, so the trace batch is usually much smaller
        if let Some(content) = payload.logs.take() {
            traces.push(TraceRow {
                timestamp: payload.timestamp.into(),
                id: payload.id.clone(),
                app: payload.app.clone(),
                error_code: payload.error_code.clone(),
                content,
            });
        }
        if let Some(service) = &state.alert_service {
            service.process_action(&payload);
        }
        actions.push(to_action_row(payload));
    }

    state.clickhouse.insert("action_rs", &actions).await?;
    if !traces.is_empty() {
        state.clickhouse.insert("trace_rs", &traces).await?;
    }
    Ok(())
}

fn to_action_row(payload: ActionMessage) -> ActionRow {
    // a single value goes into context; multiple values go into multi_context
    let mut context: HashMap<String, String> = HashMap::new();
    let mut multi_context: HashMap<String, Vec<String>> = HashMap::new();
    for (key, mut values) in payload.context {
        if values.len() == 1 {
            context.insert(key, values.swap_remove(0));
        } else {
            multi_context.insert(key, values);
        }
    }

    // a single ref_id goes into ref_id; multiple ref_ids go into ref_ids
    let (ref_id, ref_ids) = match payload.ref_ids {
        Some(mut ids) if ids.len() == 1 => (Some(ids.swap_remove(0)), Vec::new()),
        Some(ids) => (None, ids),
        None => (None, Vec::new()),
    };

    ActionRow {
        timestamp: payload.timestamp.into(),
        id: payload.id,
        app: payload.app,
        host: payload.host,
        severity: payload.severity.into(),
        kind: payload.kind,
        ref_id,
        ref_ids,
        error_code: payload.error_code,
        error_message: payload.error_message,
        context,
        multi_context,
        stats: payload.stats.into_iter().collect(),
    }
}

#[cfg(test)]
mod tests {
    use framework::appender::ActionMessage;
    use framework::log::Severity;
    use framework::time::DateTime;

    use super::to_action_row;

    fn message() -> ActionMessage {
        ActionMessage {
            id: "id".to_owned(),
            timestamp: DateTime::now(),
            app: "app".to_owned(),
            host: "host".to_owned(),
            kind: "message".to_owned(),
            severity: Severity::Info,
            ref_ids: None,
            error_code: None,
            error_message: None,
            context: vec![],
            stats: vec![],
            logs: None,
        }
    }

    #[test]
    fn split_context() {
        let mut action = message();
        action.context = vec![
            ("subject".to_owned(), vec!["log.action".to_owned()]),
            ("client".to_owned(), vec!["a".to_owned(), "b".to_owned()]),
            ("empty".to_owned(), vec![]),
        ];

        let row = to_action_row(action);

        assert_eq!(row.context.get("subject"), Some(&"log.action".to_owned()));
        assert_eq!(row.multi_context.get("client"), Some(&vec!["a".to_owned(), "b".to_owned()]));
        assert_eq!(row.multi_context.get("empty"), Some(&vec![]));
    }

    #[test]
    fn single_ref_id() {
        let mut action = message();
        action.ref_ids = Some(vec!["1".to_owned()]);

        let row = to_action_row(action);

        assert_eq!(row.ref_id, Some("1".to_owned()));
        assert!(row.ref_ids.is_empty());
    }

    #[test]
    fn multiple_ref_ids() {
        let mut action = message();
        action.ref_ids = Some(vec!["1".to_owned(), "2".to_owned()]);

        let row = to_action_row(action);

        assert_eq!(row.ref_id, None);
        assert_eq!(row.ref_ids, ["1".to_owned(), "2".to_owned()]);
    }

    #[test]
    fn without_ref_id() {
        let row = to_action_row(message());

        assert_eq!(row.ref_id, None);
        assert!(row.ref_ids.is_empty());
    }
}
