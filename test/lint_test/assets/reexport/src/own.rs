mod inner;

// what the children of a module define is the module's to re-export
pub use inner::Kept;
// lint: expect(foreign_reexport)
pub use inner::Kept as Unneeded;
pub use std::fmt::Display;
