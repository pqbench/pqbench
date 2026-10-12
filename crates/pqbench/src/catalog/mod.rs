//! The catalog command: one catalog's record and the catalogs at the endpoint.
//!
//! The catalog is the entity above schemas. [`get`] reads one catalog's
//! record; [`ls`] lists the catalogs at the endpoint, one `Catalog` each.

pub mod get;
pub mod ls;
