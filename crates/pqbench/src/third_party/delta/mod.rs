//! Delta snapshot resolution, isolated from the rest of pqbench.
//!
//! [`api`] is the public surface; the private `impl` module is the only file
//! that names the `deltalake` crate.

pub mod api;
mod r#impl;

pub use api::{load, visit_load};
