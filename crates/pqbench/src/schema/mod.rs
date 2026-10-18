//! The schema command: one schema's record and the schemas in a catalog.
//!
//! The schema is the entity between catalogs and tables. [`get`] reads one
//! schema's record; [`list`] lists the schemas in a catalog, one `Schema` each.

pub mod get;
pub mod list;

pub use crate::config::Dialect as TableFormat;
