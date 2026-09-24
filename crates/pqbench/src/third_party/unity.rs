//! Unity Catalog / Databricks lake listing, isolated from the rest of pqbench.
//!
//! [`api`] is the public surface; the private `impl` module is the only file
//! that names `reqwest`. The module is always compiled; without the `unity`
//! feature [`api::list_tables`] fails at runtime.

pub mod api;
mod r#impl;

pub use api::{list_tables, Error, LakeSource, NameFilter};
