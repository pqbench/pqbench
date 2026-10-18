//! `catalog::get`: one catalog's record.
//!
//! [`api`] is the public surface: one function, [`api::get`]. The private
//! `impl` module holds the URL and the JSON shape; the HTTP transport is the
//! third-party facade. The wire document belongs to the CLI.

pub mod api;
mod r#impl;

pub use api::{get, Catalog, Error};
