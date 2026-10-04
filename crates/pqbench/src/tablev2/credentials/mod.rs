//! `credentials get`: one table's vended storage options.
//!
//! [`api`] is the public surface: one function, [`api::vend`]. It asks Unity
//! for temporary read credentials and returns the `AWS_*` storage options for
//! the table, or `None` when the table's kind cannot be read outside Databricks
//! compute or the catalog does not serve the route, so the caller keeps its own
//! `env`. A catalog that reports no kind is attempted: the route itself
//! decides. The private `impl` module holds the URLs and the JSON shapes; the
//! transport is the third-party facade.

pub mod api;
mod r#impl;

pub use api::{vend, Error};
