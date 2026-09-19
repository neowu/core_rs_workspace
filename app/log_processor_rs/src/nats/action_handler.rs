use std::borrow::Cow;
use std::sync::Arc;

use framework::appender::ActionMessage;
use framework::exception::Exception;
use framework::log::ContextValues;
use framework_clickhouse::clickhouse;
use framework_clickhouse::clickhouse::Row;
use framework_clickhouse::types::DateTime64;
use framework_clickhouse::types::Map;
use framework_nats::consumer::Message;
use serde::Serialize;
use serde::Serializer;
use serde::ser::SerializeMap as _;

use crate::AppState;
use crate::nats::Severity;

// the row borrows from the message it is built from, and the map columns serialize straight from
// the message's own ordered pairs, so a batch is inserted without copying a string or building a
// HashMap per row
#[derive(Row, Serialize)]
struct ActionRow<'a> {
    timestamp: DateTime64,
    id: &'a str,
    app: &'a str,
    host: &'a str,
    severity: Severity,
    kind: &'a str,
    ref_id: Option<&'a str>,
    ref_ids: &'a [String],
    error_code: Option<&'a str>,
    error_message: Option<&'a str>,
    context: SingleContext<'a>,
    multi_context: MultiContext<'a>,
    stats: Map<'a, Cow<'static, str>, u64>,
}

#[derive(Row, Serialize)]
struct TraceRow<'a> {
    timestamp: DateTime64,
    id: &'a str,
    app: &'a str,
    error_code: Option<&'a str>,
    content: &'a str,
}

type Context = (Cow<'static, str>, ContextValues);

// the two halves of the context split are two views over the same slice rather than two maps, so
// neither half is materialized; each counts its own entries first, since RowBinary needs the
// length up front
#[derive(Debug)]
struct SingleContext<'a>(&'a [Context]);

#[derive(Debug)]
struct MultiContext<'a>(&'a [Context]);

impl Serialize for SingleContext<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.0.iter().filter(|(_, values)| values.len() == 1).count()))?;
        for (key, values) in self.0 {
            if let [value] = values.as_slice() {
                map.serialize_entry(key.as_ref(), value)?;
            }
        }
        map.end()
    }
}

impl Serialize for MultiContext<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.0.iter().filter(|(_, values)| values.len() != 1).count()))?;
        for (key, values) in self.0 {
            if values.len() != 1 {
                map.serialize_entry(key.as_ref(), values.as_slice())?;
            }
        }
        map.end()
    }
}

pub(crate) async fn action_message_handler(
    state: Arc<AppState>,
    messages: Vec<Message<ActionMessage>>,
) -> Result<(), Exception> {
    let mut actions = Vec::with_capacity(messages.len());
    let mut traces = vec![];
    for message in &messages {
        let payload = &message.payload;
        // logs are only carried by traced or failed actions, so the trace batch is usually much smaller
        if let Some(content) = payload.logs.as_deref() {
            traces.push(TraceRow {
                timestamp: payload.timestamp.into(),
                id: &payload.id,
                app: &payload.app,
                error_code: payload.error_code.as_deref(),
                content,
            });
        }
        if let Some(service) = &state.alert_service {
            service.process_action(payload);
        }
        actions.push(to_action_row(payload));
    }

    state.clickhouse.insert_borrowed::<ActionRow>("action_rs", &actions).await?;
    if !traces.is_empty() {
        state.clickhouse.insert_borrowed::<TraceRow>("trace_rs", &traces).await?;
    }
    Ok(())
}

fn to_action_row(payload: &ActionMessage) -> ActionRow<'_> {
    // a single ref_id goes into ref_id; multiple ref_ids go into ref_ids
    let (ref_id, ref_ids) = match payload.ref_ids.as_deref() {
        Some([id]) => (Some(id.as_str()), [].as_slice()),
        Some(ids) => (None, ids),
        None => (None, [].as_slice()),
    };

    ActionRow {
        timestamp: payload.timestamp.into(),
        id: &payload.id,
        app: &payload.app,
        host: &payload.host,
        severity: payload.severity.into(),
        kind: &payload.kind,
        ref_id,
        ref_ids,
        error_code: payload.error_code.as_deref(),
        error_message: payload.error_message.as_deref(),
        // a single value goes into context; multiple values go into multi_context
        context: SingleContext(&payload.context),
        multi_context: MultiContext(&payload.context),
        stats: Map(&payload.stats),
    }
}

#[cfg(test)]
mod tests {
    use framework::appender::ActionMessage;
    use framework::json;
    use framework::log::Severity;
    use framework::time::DateTime;

    use super::to_action_row;

    fn message() -> ActionMessage {
        ActionMessage {
            id: "id".to_owned(),
            timestamp: DateTime::now(),
            app: "app".into(),
            host: "host".into(),
            kind: "message".into(),
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
            ("subject".into(), vec!["log.action".to_owned()].into()),
            ("client".into(), vec!["a".to_owned(), "b".to_owned()].into()),
            ("empty".into(), vec![].into()),
        ];

        let row = to_action_row(&action);

        assert_eq!(json::to_json(&row.context).unwrap(), r#"{"subject":"log.action"}"#);
        assert_eq!(json::to_json(&row.multi_context).unwrap(), r#"{"client":["a","b"],"empty":[]}"#);
    }

    // a repeated context key stays a repeated map entry rather than overwriting, which is what the
    // action's own context does; clickhouse resolves a lookup to the first one
    #[test]
    fn repeated_context_key() {
        let mut action = message();
        action.context =
            vec![("path".into(), vec!["/a".to_owned()].into()), ("path".into(), vec!["/b".to_owned()].into())];

        let row = to_action_row(&action);

        assert_eq!(json::to_json(&row.context).unwrap(), r#"{"path":"/a","path":"/b"}"#);
    }

    #[test]
    fn deserialized_metadata_and_stats() {
        let mut action = message();
        action.stats.push(("elapsed".into(), 42));
        action.context.push(("subject".into(), vec!["log.action".to_owned()].into()));
        let encoded = json::to_json(&action).unwrap();
        let decoded: ActionMessage = json::from_json(&encoded).unwrap();

        let row = to_action_row(&decoded);

        assert_eq!(row.app, "app");
        assert_eq!(row.host, "host");
        assert_eq!(row.kind, "message");
        assert_eq!(json::to_json(&row.stats).unwrap(), r#"{"elapsed":42}"#);
        assert_eq!(json::to_json(&row.context).unwrap(), r#"{"subject":"log.action"}"#);
    }

    #[test]
    fn single_ref_id() {
        let mut action = message();
        action.ref_ids = Some(vec!["1".to_owned()]);

        let row = to_action_row(&action);

        assert_eq!(row.ref_id, Some("1"));
        assert!(row.ref_ids.is_empty());
    }

    #[test]
    fn multiple_ref_ids() {
        let mut action = message();
        action.ref_ids = Some(vec!["1".to_owned(), "2".to_owned()]);

        let row = to_action_row(&action);

        assert_eq!(row.ref_id, None);
        assert_eq!(row.ref_ids, ["1".to_owned(), "2".to_owned()]);
    }

    #[test]
    fn without_ref_id() {
        let action = message();

        let row = to_action_row(&action);

        assert_eq!(row.ref_id, None);
        assert!(row.ref_ids.is_empty());
    }
}
