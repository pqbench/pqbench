//! `metastore ls`: the catalogs at the endpoint.
//!
//! [`api`] is the public surface: one function, [`api::list`]. The private
//! `impl` module holds the URL, the page shape, and the pagination; the HTTP
//! transport is the third-party facade. The wire document belongs to the CLI.

pub mod api;
mod r#impl;

pub use api::{list, Catalog, Error};
