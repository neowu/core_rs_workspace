use serde::Deserialize;
use serde::Serialize;

use crate::log::Severity;

#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub severity: Severity,
    pub code: Option<String>,
    pub message: String,
}

pub use definition::ApiDefinition;
pub use definition::ApiType;
pub use definition::Constraints;
pub use definition::FieldDefinition;
pub use definition::OperationDefinition;
pub use definition::ServiceDefinition;
pub use definition::TypeDefinition;
pub use definition::TypeRef;
pub use definition::TypeRegistry;

mod definition;
