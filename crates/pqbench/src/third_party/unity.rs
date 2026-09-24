//! Unity Catalog / Iceberg REST lake listing, isolated from the rest of pqbench.
//!
//! [`api`] is the public surface; the private `impl` module and the backends it
//! declares are the only files that name `reqwest` and the only place feature
//! flags live. This root is always compiled; without the `unity` feature
//! [`api::list_tables`] fails at runtime.

pub mod api;
mod r#impl;

pub use api::{list_tables, Error, LakeSource, NameFilter};
