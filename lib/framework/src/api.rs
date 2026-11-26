use serde::Deserialize;
use serde::Serialize;

use crate::exception::Severity;

#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub severity: Severity,
    pub code: Option<String>,
    pub message: String,
}
