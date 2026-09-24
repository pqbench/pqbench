//! Unity Catalog / Iceberg REST lake listing, isolated from the rest of pqbench.
//!
//! [`api`] is the public surface; the private backends (`impl`, `client`,
//! `filter`, `http`, `protocol`, `iceberg_rest`) are the only files that name
//! `reqwest`. The module is always compiled; without the `unity` feature
//! [`api::list_tables`] fails at runtime.

pub mod api;
mod r#impl;

#[cfg(feature = "unity")]
mod client;
#[cfg(feature = "unity")]
mod filter;
#[cfg(feature = "unity")]
mod http;
#[cfg(feature = "unity")]
mod iceberg_rest;
#[cfg(feature = "unity")]
mod protocol;

pub use api::{list_tables, Error, LakeSource, NameFilter};
