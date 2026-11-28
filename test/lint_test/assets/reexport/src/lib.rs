mod other;
mod own;

// a crate of the workspace is named by its users, not handed on
pub use visibility::a::five;
// lint: expect(foreign_reexport), a facade on purpose
pub use visibility::a::eight;
// a crate from outside the workspace can be handed on
pub use std::fmt::Debug;
