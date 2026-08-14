use std::collections::HashMap;

use framework::time::DateTime;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct EventMessage {
    pub id: String,
    pub timestamp: DateTime, // server received_time
    pub app: String,
    pub client_timestamp: DateTime,
    pub result: String,
    pub action: String,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub elapsed: i64,
    pub context: HashMap<String, String>,
    pub stats: Option<HashMap<String, f64>>,
    pub info: Option<HashMap<String, String>>,
}
