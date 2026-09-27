//! `catalog ls`: the schemas in one catalog.
//!
//! [`api`] is the public surface: one function, [`api::list`]. The private
//! `impl` module holds the URLs and the JSON shapes of both dialects — Unity
//! REST and Iceberg REST, declared by the caller as a [`api::TableFormat`]; the
//! HTTP transport is the third-party facade. The wire document belongs to the
//! CLI.

pub mod api;
mod r#impl;

pub use api::{list, Error, Schema, TableFormat};
