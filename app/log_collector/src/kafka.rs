use std::collections::HashMap;

use chrono::DateTime;
use chrono::Utc;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Serialize, Deserialize)]
pub struct EventMessage {
    pub id: String,
    pub date: DateTime<Utc>,
    pub app: String,
    pub received_time: DateTime<Utc>,
    pub result: String,
    pub action: String,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub elapsed: i64,
    pub context: HashMap<String, String>,
    pub stats: Option<HashMap<String, f64>>,
    pub info: Option<HashMap<String, String>>,
}
