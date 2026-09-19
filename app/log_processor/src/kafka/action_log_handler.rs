use std::collections::HashMap;
use std::sync::Arc;

use framework::exception::Exception;
use framework::time::Date;
use framework::time::DateTime;
use framework::write_str;
use framework_clickhouse::ClickHouse;
use framework_clickhouse::Enum8;
use framework_clickhouse::clickhouse;
use framework_clickhouse::clickhouse::Row;
use framework_clickhouse::types::DateTime64;
use framework_clickhouse::types::Decimal64;
use framework_kafka::consumer::Message;
use serde::Deserialize;
use serde::Serialize;
use serde::Serializer;
use serde::ser::SerializeMap as _;
use serde::ser::SerializeSeq as _;

use crate::AppState;
use crate::elasticsearch::Elasticsearch;

// action log message schema from java core-ng framework
#[derive(Debug, Deserialize)]
pub(crate) struct ActionLogMessage {
    id: String,
    date: DateTime,
    app: String,
    host: String,
    result: String,
    action: String,
    correlation_ids: Option<Vec<String>>,
    clients: Option<Vec<String>>,
    ref_ids: Option<Vec<String>>,
    error_code: Option<String>,
    error_message: Option<String>,
    elapsed: i64,
    context: HashMap<String, Vec<Option<String>>>,
    stats: Option<HashMap<String, f64>>,
    perf_stats: Option<HashMap<String, PerformanceStatMessage>>,
    trace_log: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct PerformanceStatMessage {
    total_elapsed: i64,
    count: i64,
    read_entries: Option<i64>,
    write_entries: Option<i64>,
    read_bytes: Option<i64>,
    write_bytes: Option<i64>,
}

impl PerformanceStatMessage {
    // the suffix each counter takes under the perf category's key; the four optional ones are
    // absent for categories that do not track them
    fn counters(&self) -> impl Iterator<Item = (&'static str, i64)> + Clone {
        [
            Some(("elapsed", self.total_elapsed)),
            Some(("count", self.count)),
            self.read_entries.map(|value| ("read_entries", value)),
            self.write_entries.map(|value| ("write_entries", value)),
            self.read_bytes.map(|value| ("read_bytes", value)),
            self.write_bytes.map(|value| ("write_bytes", value)),
        ]
        .into_iter()
        .flatten()
    }
}

pub(crate) async fn action_log_message_handler(
    state: Arc<AppState>,
    messages: Vec<Message<ActionLogMessage>>,
) -> Result<(), Exception> {
    if let Some(clickhouse) = &state.clickhouse {
        insert_to_clickhouse(clickhouse, &messages).await?;
    }

    index_to_elasticsearch(&state.elasticsearch, messages).await?;
    Ok(())
}

#[derive(Debug, Serialize)]
struct ActionLogDocument {
    #[serde(rename = "@timestamp")]
    timestamp: DateTime,
    app: String,
    host: String,
    result: String,
    action: String,
    #[serde(rename = "correlation_id")]
    correlation_ids: Option<Vec<String>>,
    #[serde(rename = "client")]
    clients: Option<Vec<String>>,
    #[serde(rename = "ref_id")]
    ref_ids: Option<Vec<String>>,
    error_code: Option<String>,
    error_message: Option<String>,
    elapsed: i64,
    context: HashMap<String, Vec<Option<String>>>,
    stats: Option<HashMap<String, f64>>,
    perf_stats: Option<HashMap<String, PerformanceStatMessage>>,
}

#[derive(Debug, Serialize)]
struct TraceDocument {
    #[serde(rename = "@timestamp")]
    timestamp: DateTime,
    app: String,
    result: String,
    action: String,
    error_code: Option<String>,
    content: String,
}

async fn index_to_elasticsearch(
    elasticsearch: &Elasticsearch,
    messages: Vec<Message<ActionLogMessage>>,
) -> Result<(), Exception> {
    let mut documents: Vec<(String, ActionLogDocument)> = Vec::with_capacity(messages.len());
    let mut traces: Vec<(String, TraceDocument)> = vec![];
    for message in messages {
        let payload = message.payload;
        let doc = ActionLogDocument {
            timestamp: payload.date,
            app: payload.app.clone(),
            host: payload.host,
            result: payload.result.clone(),
            action: payload.action.clone(),
            correlation_ids: payload.correlation_ids,
            clients: payload.clients,
            ref_ids: payload.ref_ids,
            error_code: payload.error_code.clone(),
            error_message: payload.error_message,
            elapsed: payload.elapsed,
            context: payload.context,
            stats: payload.stats,
            perf_stats: payload.perf_stats,
        };
        documents.push((payload.id.clone(), doc));

        if let Some(content) = payload.trace_log {
            let trace_doc = TraceDocument {
                timestamp: payload.date,
                app: payload.app,
                result: payload.result,
                action: payload.action,
                error_code: payload.error_code,
                content,
            };
            traces.push((payload.id, trace_doc));
        }
    }
    let now = DateTime::now().date();
    elasticsearch.bulk_index(&action_index(now), documents).await?;
    if !traces.is_empty() {
        elasticsearch.bulk_index(&trace_index(now), traces).await?;
    }
    Ok(())
}

fn action_index(now: Date) -> String {
    let (year, month, day) = now.to_ymd();
    format!("action-{year}.{month:02}.{day:02}")
}

fn trace_index(now: Date) -> String {
    let (year, month, day) = now.to_ymd();
    format!("trace-{year}.{month:02}.{day:02}")
}

#[derive(Row, Serialize)]
struct ActionRow<'a> {
    pub timestamp: DateTime64,
    pub id: &'a str,
    pub app: &'a str,
    pub host: &'a str,
    pub result: ActionResult,
    pub action: &'a str,
    pub ref_id: Option<&'a str>,
    pub ref_ids: &'a [String],
    pub error_code: Option<&'a str>,
    pub error_message: Option<&'a str>,
    pub context: SingleContext<'a>,
    pub multi_context: MultiContext<'a>,
    pub stats: ActionStats<'a>,
}

// Enum8('OK' = 1, 'WARN' = 2, 'ERROR' = 3)
#[derive(Enum8)]
enum ActionResult {
    Ok = 1,
    Warn = 2,
    Error = 3,
}

#[derive(Row, Serialize)]
struct TraceRow<'a> {
    pub timestamp: DateTime64,
    pub id: &'a str,
    pub app: &'a str,
    pub error_code: Option<&'a str>,
    pub content: &'a str,
}

// a single value goes into context; multiple values go into multi_context, dropping the nulls java
// core-ng may send. `clients` is a field of its own on the message and joins the split under the
// "client" key. Both halves are views over the same message rather than two maps built up front.
struct SingleContext<'a>(&'a ActionLogMessage);

struct MultiContext<'a>(&'a ActionLogMessage);

const fn is_single(values: &[Option<String>]) -> bool {
    matches!(values, [Some(_)])
}

impl Serialize for SingleContext<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let payload = self.0;
        let clients = payload.clients.as_deref().unwrap_or_default();
        let entries =
            payload.context.values().filter(|values| is_single(values)).count() + usize::from(clients.len() == 1);
        let mut map = serializer.serialize_map(Some(entries))?;
        for (key, values) in &payload.context {
            if let [Some(value)] = values.as_slice() {
                map.serialize_entry(key, value)?;
            }
        }
        if let [client] = clients {
            map.serialize_entry("client", client)?;
        }
        map.end()
    }
}

impl Serialize for MultiContext<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let payload = self.0;
        let clients = payload.clients.as_deref();
        let entries = payload.context.values().filter(|values| !is_single(values)).count()
            + usize::from(clients.is_some_and(|clients| clients.len() != 1));
        let mut map = serializer.serialize_map(Some(entries))?;
        for (key, values) in &payload.context {
            if !is_single(values) {
                map.serialize_entry(key, &Values(values))?;
            }
        }
        if let Some(clients) = clients
            && clients.len() != 1
        {
            map.serialize_entry("client", clients)?;
        }
        map.end()
    }
}

// a context value java core-ng sent as null is dropped rather than stored
struct Values<'a>(&'a [Option<String>]);

impl Serialize for Values<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.0.iter().flatten().count()))?;
        for value in self.0.iter().flatten() {
            seq.serialize_element(value)?;
        }
        seq.end()
    }
}

// elapsed and every perf_stat counter are flattened into the one numeric stats map as the row is
// written, so no map is collected; the six keys per perf category are formatted into one reused
// buffer rather than a String each.
struct ActionStats<'a>(&'a ActionLogMessage);

impl Serialize for ActionStats<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let payload = self.0;
        let perf_stats = payload.perf_stats.iter().flatten();
        let entries = 1
            + payload.stats.as_ref().map_or(0, HashMap::len)
            + perf_stats.clone().map(|(_, perf)| perf.counters().count()).sum::<usize>();
        let mut map = serializer.serialize_map(Some(entries))?;
        map.serialize_entry("elapsed", &Decimal64::<3>::from(payload.elapsed as f64))?;
        for (key, value) in payload.stats.iter().flatten() {
            map.serialize_entry(key, &Decimal64::<3>::from(*value))?;
        }
        let mut name = String::new();
        for (key, perf) in perf_stats {
            for (suffix, value) in perf.counters() {
                name.clear();
                write_str!(&mut name, "{key}_{suffix}");
                map.serialize_entry(name.as_str(), &Decimal64::<3>::from(value as f64))?;
            }
        }
        map.end()
    }
}

async fn insert_to_clickhouse(
    clickhouse: &ClickHouse,
    messages: &[Message<ActionLogMessage>],
) -> Result<(), Exception> {
    let mut actions = Vec::with_capacity(messages.len());
    let mut traces = vec![];
    for message in messages {
        let payload = &message.payload;
        actions.push(to_action_row(payload));
        if let Some(content) = payload.trace_log.as_deref() {
            let trace = TraceRow {
                timestamp: payload.date.into(),
                id: &payload.id,
                content,
                app: &payload.app,
                error_code: payload.error_code.as_deref(),
            };
            traces.push(trace);
        }
    }

    clickhouse.insert_borrowed::<ActionRow>("action", &actions).await?;
    if !traces.is_empty() {
        clickhouse.insert_borrowed::<TraceRow>("trace", &traces).await?;
    }
    Ok(())
}

fn to_action_row(payload: &ActionLogMessage) -> ActionRow<'_> {
    // a single ref_id goes into ref_id; multiple ref_ids go into ref_ids.
    let (ref_id, ref_ids) = match payload.ref_ids.as_deref() {
        Some([id]) => (Some(id.as_str()), [].as_slice()),
        Some(ids) => (None, ids),
        None => (None, [].as_slice()),
    };

    ActionRow {
        timestamp: payload.date.into(),
        id: &payload.id,
        app: &payload.app,
        host: &payload.host,
        result: to_action_result(&payload.result),
        action: &payload.action,
        ref_id,
        ref_ids,
        error_code: payload.error_code.as_deref(),
        error_message: payload.error_message.as_deref(),
        context: SingleContext(payload),
        multi_context: MultiContext(payload),
        stats: ActionStats(payload),
    }
}

fn to_action_result(result: &str) -> ActionResult {
    match result {
        "WARN" => ActionResult::Warn,
        "ERROR" => ActionResult::Error,
        _ => ActionResult::Ok,
    }
}

#[cfg(test)]
mod tests {
    use framework::time::Date;

    #[test]
    fn action_index() {
        assert_eq!(super::action_index(Date::new(2026, 8, 12)), "action-2026.08.12");
    }
}
