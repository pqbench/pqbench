//! The credentials domain, one module per command: [`check`] decides whether a
//! table is readable outside Databricks compute, and [`get`] vends its storage
//! options. They share no API; each owns its transport.

pub mod check;
pub mod get;
