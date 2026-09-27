//! `metastore info`: the endpoint's metastore record.
//!
//! [`api`] is the public surface: one function, [`api::read`]. The private
//! `impl` module holds the URL and the JSON shape; the HTTP transport is the
//! third-party facade. The wire document belongs to the CLI.

pub mod api;
mod r#impl;

pub use api::{read, Error, Metastore};
