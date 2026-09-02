use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use framework::appender::ActionMessage;
use framework::appender::MetricsMessage;
use framework::log::Severity;
use framework::spawn_action;
use framework::write_str;

use crate::alert::slack::SlackClient;

pub(crate) mod slack;

// an ERROR needs attention right away, a WARN is only worth a periodic reminder
const ERROR_INTERVAL: Duration = Duration::from_secs(60);
const WARN_INTERVAL: Duration = Duration::from_hours(4);

// bounds the state, an entry past its interval notifies on the next alert anyway, so dropping it only loses the count
const MAX_STATS: usize = 1_000;

struct Alert<'a> {
    id: &'a str,
    app: &'a str,
    severity: Severity,
    error_code: Option<&'a str>,
    error_message: Option<&'a str>,
}

struct AlertStat {
    last_sent: Instant,
    count_since_last_sent: u32,
}

/// Throttles alerts per app / severity / error_code and notifies slack.
///
/// The state is kept in memory only, one processor is assumed to consume the whole log stream.
pub(crate) struct AlertService {
    stats: Mutex<HashMap<String, AlertStat>>,
    // shared with the spawned notification task, which outlives this call
    slack: Arc<SlackClient>,
}

impl AlertService {
    pub(crate) fn new(slack: SlackClient) -> Self {
        Self { stats: Mutex::new(HashMap::new()), slack: Arc::new(slack) }
    }

    pub(crate) fn process_action(&self, message: &ActionMessage) {
        // most actions are INFO, filter before touching the lock
        if message.severity == Severity::Info {
            return;
        }
        self.process(
            &Alert {
                id: &message.id,
                app: &message.app,
                severity: message.severity,
                error_code: message.error_code.as_deref(),
                error_message: message.error_message.as_deref(),
            },
            &action_info(&message.kind, &message.context),
        );
    }

    pub(crate) fn process_metrics(&self, message: &MetricsMessage) {
        if message.severity == Severity::Info {
            return;
        }
        self.process(
            &Alert {
                id: &message.id,
                app: &message.app,
                severity: message.severity,
                error_code: message.error_code.as_deref(),
                error_message: message.error_message.as_deref(),
            },
            &format!("host: {}", message.host),
        );
    }

    // detail is the message specific line shown right under the id
    fn process(&self, alert: &Alert<'_>, info: &str) {
        let Some(count) = self.check(alert) else { return };

        let message = message(alert, info, count);
        let severity = alert.severity;
        let slack = Arc::clone(&self.slack);
        // notify out of band, slack must never slow down log ingestion nor fail the batch
        spawn_action!("notify_slack", async move { slack.send(severity, &message).await });
    }

    /// Returns how many alerts were suppressed since the last notification, or `None` to stay silent.
    fn check(&self, alert: &Alert<'_>) -> Option<u32> {
        let interval = if alert.severity == Severity::Error { ERROR_INTERVAL } else { WARN_INTERVAL };
        let key = alert_key(alert);
        let now = Instant::now();

        let mut stats = self.stats.lock().unwrap();

        if let Some(stat) = stats.get_mut(&key) {
            if now.saturating_duration_since(stat.last_sent) < interval {
                stat.count_since_last_sent += 1;
                return None;
            }
            let count = stat.count_since_last_sent;
            stat.last_sent = now;
            stat.count_since_last_sent = 0;
            return Some(count);
        }

        if stats.len() >= MAX_STATS {
            stats.retain(|_, stat| now.saturating_duration_since(stat.last_sent) < WARN_INTERVAL);
        }
        stats.insert(key, AlertStat { last_sent: now, count_since_last_sent: 0 });
        Some(0)
    }
}

// WARN and ERROR may share an error code, so severity is part of the key
fn alert_key(alert: &Alert<'_>) -> String {
    let mut key = String::with_capacity(64);
    write_str!(key, "{}/{}/{}", alert.app, alert.severity, error_code(alert));
    key
}

fn error_code<'a>(alert: &Alert<'a>) -> &'a str {
    alert.error_code.unwrap_or("UNASSIGNED")
}

fn message(alert: &Alert<'_>, info: &str, count: u32) -> String {
    let mut message = String::with_capacity(256);
    // the first notification of a key has nothing to count yet
    if count > 0 {
        write_str!(message, "[{count}] ");
    }
    write_str!(
        message,
        "{}: *{}*\nid: {}\n{}\nerror_code: *{}*\nmessage: {}\n",
        alert.severity,
        alert.app,
        alert.id,
        info,
        error_code(alert),
        alert.error_message.unwrap_or_default()
    );
    message
}

fn action_info(kind: &str, context: &[(String, Vec<String>)]) -> String {
    let mut line = String::with_capacity(64);
    write_str!(line, "kind: {kind}");
    // the interesting field differs per kind, an action carries only the ones its own framework layer set
    match kind {
        "http" => {
            // an api request names the handler, anything else (static resource, unmatched path) only has the request line
            if let Some(name) = context_value(context, "fn") {
                write_str!(line, ", fn={name}");
            } else if let Some(url) = context_value(context, "uri") {
                write_str!(line, ", method={}, url={url}", context_value(context, "method").unwrap_or_default());
            }
        }
        "message" => {
            // kafka names the topic, nats the subject
            if let Some(topic) = context_value(context, "topic") {
                write_str!(line, ", topic={topic}");
            } else if let Some(subject) = context_value(context, "subject") {
                write_str!(line, ", subject={subject}");
            }
        }
        "task" => {
            if let Some(task) = context_value(context, "task") {
                write_str!(line, ", task={task}");
            }
        }
        "nats" => {
            if let Some(name) = context_value(context, "fn") {
                write_str!(line, ", fn={name}");
            }
        }
        _ => {}
    }
    line
}

fn context_value<'a>(context: &'a [(String, Vec<String>)], key: &str) -> Option<&'a str> {
    context.iter().find(|(name, _)| name.as_str() == key).and_then(|(_, values)| values.first()).map(String::as_str)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use std::time::Instant;

    use framework::log::Severity;

    use super::AlertService;
    use super::AlertStat;
    use super::ERROR_INTERVAL;
    use super::MAX_STATS;
    use super::WARN_INTERVAL;
    use super::alert_key;
    use super::message;
    use crate::alert::Alert;
    use crate::alert::action_info;
    use crate::alert::slack::SlackClient;

    fn service() -> AlertService {
        AlertService::new(SlackClient::new("token".to_owned(), "#error".to_owned(), "#warn".to_owned()))
    }

    fn alert(severity: Severity) -> Alert<'static> {
        Alert { id: "id", app: "app", severity, error_code: Some("ERROR_CODE"), error_message: Some("error message") }
    }

    fn context(entries: &[(&str, &str)]) -> Vec<(String, Vec<String>)> {
        entries.iter().map(|(key, value)| ((*key).to_owned(), vec![(*value).to_owned()])).collect()
    }

    // moves the last sent time of every entry back, to simulate the interval passing
    fn rewind(service: &AlertService, duration: Duration) {
        let mut stats = service.stats.lock().unwrap();
        for stat in stats.values_mut() {
            stat.last_sent = stat.last_sent.checked_sub(duration).expect("instant must be in range");
        }
    }

    #[test]
    fn key_includes_severity() {
        assert_eq!(alert_key(&alert(Severity::Error)), "app/ERROR/ERROR_CODE");
        assert_eq!(alert_key(&alert(Severity::Warn)), "app/WARN/ERROR_CODE");
    }

    #[test]
    fn key_without_error_code() {
        let mut alert = alert(Severity::Error);
        alert.error_code = None;

        assert_eq!(alert_key(&alert), "app/ERROR/UNASSIGNED");
    }

    #[test]
    fn first_alert_notifies() {
        let service = service();

        assert_eq!(service.check(&alert(Severity::Error)), Some(0));
    }

    #[test]
    fn alert_within_interval_is_suppressed() {
        let service = service();
        let alert = alert(Severity::Error);

        assert_eq!(service.check(&alert), Some(0));
        assert_eq!(service.check(&alert), None);
        assert_eq!(service.check(&alert), None);
    }

    #[test]
    fn alert_after_interval_notifies_with_suppressed_count() {
        let service = service();
        let alert = alert(Severity::Error);

        service.check(&alert);
        service.check(&alert);
        service.check(&alert);
        rewind(&service, ERROR_INTERVAL);

        assert_eq!(service.check(&alert), Some(2));
        // the count restarts after each notification
        assert_eq!(service.check(&alert), None);
        rewind(&service, ERROR_INTERVAL);
        assert_eq!(service.check(&alert), Some(1));
    }

    #[test]
    fn warn_uses_longer_interval() {
        let service = service();
        let alert = alert(Severity::Warn);

        service.check(&alert);
        service.check(&alert);
        rewind(&service, ERROR_INTERVAL);

        assert_eq!(service.check(&alert), None);
        rewind(&service, WARN_INTERVAL);
        assert_eq!(service.check(&alert), Some(2));
    }

    #[test]
    fn severities_are_throttled_separately() {
        let service = service();

        assert_eq!(service.check(&alert(Severity::Error)), Some(0));
        assert_eq!(service.check(&alert(Severity::Warn)), Some(0));
    }

    #[test]
    fn expired_stats_are_evicted_when_full() {
        let service = service();
        {
            let mut stats = service.stats.lock().unwrap();
            for index in 0..MAX_STATS {
                let stat = AlertStat { last_sent: Instant::now(), count_since_last_sent: 0 };
                stats.insert(format!("app/ERROR/CODE_{index}"), stat);
            }
        }
        rewind(&service, WARN_INTERVAL);

        assert_eq!(service.check(&alert(Severity::Error)), Some(0));
        assert_eq!(service.stats.lock().unwrap().len(), 1);
    }

    #[test]
    fn message_without_count() {
        assert_eq!(
            message(&alert(Severity::Error), "kind: task", 0),
            "ERROR: *app*\nid: id\nkind: task\nerror_code: *ERROR_CODE*\nmessage: error message\n"
        );
    }

    #[test]
    fn message_with_count() {
        assert_eq!(
            message(&alert(Severity::Warn), "kind: task", 3),
            "[3] WARN: *app*\nid: id\nkind: task\nerror_code: *ERROR_CODE*\nmessage: error message\n"
        );
    }

    #[test]
    fn message_without_error() {
        let mut alert = alert(Severity::Error);
        alert.error_code = None;
        alert.error_message = None;

        assert_eq!(
            message(&alert, "kind: task", 0),
            "ERROR: *app*\nid: id\nkind: task\nerror_code: *UNASSIGNED*\nmessage: \n"
        );
    }

    #[test]
    fn info_of_api_request() {
        let context = context(&[("method", "GET"), ("uri", "/user/1"), ("fn", "UserServiceImpl::get")]);

        assert_eq!(action_info("http", &context), "kind: http, fn=UserServiceImpl::get");
    }

    #[test]
    fn info_of_non_api_request() {
        let context = context(&[("method", "GET"), ("uri", "/static/main.css")]);

        assert_eq!(action_info("http", &context), "kind: http, method=GET, url=/static/main.css");
    }

    #[test]
    fn info_of_message() {
        assert_eq!(action_info("message", &context(&[("topic", "topic")])), "kind: message, topic=topic");
        assert_eq!(action_info("message", &context(&[("subject", "subject")])), "kind: message, subject=subject");
    }

    #[test]
    fn info_of_task() {
        let context = context(&[("task", "init_clickhouse")]);

        assert_eq!(action_info("task", &context), "kind: task, task=init_clickhouse");
    }

    #[test]
    fn info_of_nats() {
        let context = context(&[("subject", "subject"), ("fn", "UserServiceImpl::get")]);

        assert_eq!(action_info("nats", &context), "kind: nats, fn=UserServiceImpl::get");
    }

    #[test]
    fn info_without_context() {
        assert_eq!(action_info("http", &[]), "kind: http");
    }
}
