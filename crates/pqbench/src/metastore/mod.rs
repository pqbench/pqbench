//! The metastore command: one endpoint's metastore and its catalogs.
//!
//! The metastore is the entity above catalogs. [`info`] reads its record;
//! [`ls`] lists the catalogs at the endpoint.

pub mod info;
pub mod ls;
