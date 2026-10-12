//! The schema command: one schema's record and the schemas in a catalog.
//!
//! The schema is the entity between catalogs and tables. [`get`] reads one
//! schema's record; [`ls`] lists the schemas in a catalog, one `Schema` each.

pub mod get;
pub mod ls;

pub use crate::config::Dialect as TableFormat;
