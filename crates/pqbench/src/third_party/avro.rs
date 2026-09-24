//! Avro record reading, isolated from the rest of pqbench.
//!
//! [`api`] is the public surface; the private `impl` module is the only file
//! that names `apache-avro`. The module is always compiled; without the
//! `iceberg` feature [`api::read_avro`] fails at runtime.

pub mod api;
mod r#impl;

pub use api::{read_avro, Error};
