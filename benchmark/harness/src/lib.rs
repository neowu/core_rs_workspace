//! What a benchmark needs on both sides of the wire, independent of the protocol under test: the
//! latency recorder and record line every client prints, and the counting allocator every server
//! can opt into.

#[cfg(feature = "alloc_stats")]
pub mod alloc_stats;
pub mod stats;
