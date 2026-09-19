//! Compatibility API for local Delta snapshot storage analysis.
//!
//! The implementation now lives in [`pqbench::table::delta`]. New callers can
//! enable pqbench's `delta` feature and use that module directly.

pub use pqbench::table::delta::*;
