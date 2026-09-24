//! Object storage access, isolated from the rest of pqbench.
//!
//! [`api`] is the isolated surface; the private `impl` module is the only file
//! that names the `object_store` crate. The module is always compiled; an `s3`
//! URI without the `aws` feature fails at runtime.

pub(crate) mod api;
mod r#impl;

pub(crate) use api::{open, Error};
