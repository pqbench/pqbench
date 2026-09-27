//! Unity Catalog / Iceberg REST lake listing.
//!
//! [`api`] is the public surface; the private `impl` module and the backends
//! it declares hold the walks. The HTTP transport is the `reqwest` wrapper.

pub mod api;
mod r#impl;

pub use api::{list_tables, LakeSource, NameFilter};
