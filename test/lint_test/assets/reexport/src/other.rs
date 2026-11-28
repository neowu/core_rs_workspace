// a sibling does not own it, also when the path goes through another re-export
pub use crate::own::Kept;
pub(crate) use crate::own::Kept as Narrow;
pub use visibility::a::nine;
