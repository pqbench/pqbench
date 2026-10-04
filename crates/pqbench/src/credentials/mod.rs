//! `credentials get`: one table's vended storage options.
//!
//! [`api`] is the public surface: [`api::check_unity`] reports whether Unity
//! marks the table readable outside Databricks compute; [`api::vend_unity`]
//! asks Unity for temporary read credentials; [`api::vend_iceberg`] takes the
//! Iceberg REST catalog's `storage-credentials`. The vend functions return the
//! `AWS_*` storage options for the table, or `None` when the catalog does not
//! vend (managed default storage, a view, no vending support), so the caller
//! keeps its own `env`. The private `impl` module holds the URLs and the JSON
//! shapes; the transport is the third-party facade.

pub mod api;
mod r#impl;

pub use api::{check_unity, vend_iceberg, vend_unity, Eligibility, Error};
