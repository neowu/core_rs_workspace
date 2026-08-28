use serde::Deserialize;
use serde::Serialize;

use crate::log::Severity;
use crate::log::action::Action;
use crate::system::CONTEXT;
use crate::time::DateTime;

pub(crate) mod console;
pub(crate) mod gcloud;

pub trait ActionAppender: Send + 'static {
    fn append(&self, action: ActionMessage) -> impl Future<Output = ()> + Send;
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ActionMessage {
    pub id: String,
    pub timestamp: DateTime,
    pub app: String,
    pub host: String,
    pub kind: String,
    pub severity: Severity,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ref_ids: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
    pub context: Vec<(String, Vec<String>)>,
    pub stats: Vec<(String, u64)>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logs: Option<String>,
}

impl From<Action> for ActionMessage {
    fn from(action: Action) -> Self {
        let context = CONTEXT.get().expect("context must be initialized");

        let logs = action.flush_trace().then(|| action.logs.join("\n"));

        let (error_code, error_message) = match action.error {
            Some(error) => (error.code.map(str::to_owned), Some(error.message)),
            None => (None, None),
        };

        ActionMessage {
            id: action.id,
            timestamp: action.timestamp,
            app: context.app.to_owned(),
            host: context.host.clone(),
            kind: action.kind.to_owned(),
            severity: action.severity,
            ref_ids: action.ref_ids,
            error_code,
            error_message,
            context: action.context,
            stats: action.stats.into_iter().collect::<Vec<_>>(),
            logs,
        }
    }
}
