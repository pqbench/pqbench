//! Object storage access, isolated from the rest of pqbench.
//!
//! [`api`] is the isolated surface; the private `impl` module is the only file
//! that names the `object_store` crate. The S3 backend is compiled behind the
//! `aws` feature; `open` fails at runtime for an `s3` URI without it.

pub(crate) mod api;
#[cfg(feature = "aws")]
pub(crate) mod r#impl;

pub(crate) use api::{open, Error};
