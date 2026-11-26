use std::collections::HashMap;

use framework::time::DateTime;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct EventMessage {
    pub(crate) id: String,
    pub(crate) timestamp: DateTime, // server received_time
    pub(crate) app: String,
    pub(crate) client_timestamp: DateTime,
    pub(crate) result: String,
    pub(crate) action: String,
    pub(crate) error_code: Option<String>,
    pub(crate) error_message: Option<String>,
    pub(crate) elapsed: i64,
    pub(crate) context: HashMap<String, String>,
    pub(crate) stats: Option<HashMap<String, f64>>,
    pub(crate) info: Option<HashMap<String, String>>,
}
