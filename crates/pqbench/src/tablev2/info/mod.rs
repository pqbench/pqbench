//! `tablev2 info`: one table's record.
//!
//! [`api`] is the public surface: one function, [`api::read`]. The private
//! `impl` module holds the URLs and the JSON shapes of both dialects — Unity
//! REST and Iceberg REST, declared by the caller, not probed. The wire
//! document belongs to the CLI.

pub mod api;
mod r#impl;

pub use api::{read, Error, Storage};
