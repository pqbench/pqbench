//! Iceberg snapshot resolution, isolated from the rest of pqbench.
//!
//! [`api`] is the public surface; the private `impl` module is the only file
//! that names Iceberg metadata and Avro. The module is always compiled; without
//! the `iceberg` feature [`api::load`] fails at runtime.

pub mod api;
mod r#impl;

pub use api::{load, visit_load};
