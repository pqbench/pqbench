//! Unity Catalog / Databricks lake listing, isolated from the rest of pqbench.
//!
//! [`api`] is the public surface; the private `impl` module is the only file
//! that names `reqwest`. It is compiled only with the `unity` feature, and
//! without it [`api::list_tables`] fails at runtime.

pub mod api;
#[cfg(feature = "unity")]
mod r#impl;

pub use api::{list_tables, Error, LakeSource, NameFilter};
