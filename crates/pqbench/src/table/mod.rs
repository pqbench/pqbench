//! Table-format orchestration built on physical Parquet analysis.
//!
//! Enable the `delta` feature to inspect local Delta snapshots. That feature
//! requires Rust 1.91.1 or newer because of the Delta snapshot dependencies.

#[cfg(feature = "delta")]
pub mod delta;
