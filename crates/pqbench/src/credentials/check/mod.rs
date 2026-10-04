//! `credentials check`: whether a table is readable outside Databricks compute.

pub mod api;
mod r#impl;

pub use api::{check, Eligibility, Error, TableFormat};
