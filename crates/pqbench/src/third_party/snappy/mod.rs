//! Snappy (C snappy 1.2.2), isolated from the rest of pqbench.
//!
//! [`api`] is the public surface; the private `impl` module is the only file
//! that names the `snappy_src` crate.

pub mod api;
mod r#impl;

pub use api::{compress, decompress, Error};
