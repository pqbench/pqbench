//! The schema command: one schema's record and the tables in it.
//!
//! The schema is the entity between catalogs and tables. [`info`] reads its
//! record; [`ls`] lists the tables in it as `Table` resources.

pub mod info;
pub mod ls;

pub use crate::config::Dialect as TableFormat;
