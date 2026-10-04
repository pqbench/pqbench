//! `credentials get`: one table's vended storage options.

pub mod api;
mod r#impl;

pub use api::{vend_iceberg, vend_unity, Error};
