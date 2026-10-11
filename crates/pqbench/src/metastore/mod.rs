//! The metastore command: one endpoint's metastore and its catalogs.
//!
//! The metastore is the entity above catalogs. [`get`] reads its record;
//! [`ls`] lists the catalogs at the endpoint.

pub mod get;
pub mod ls;
