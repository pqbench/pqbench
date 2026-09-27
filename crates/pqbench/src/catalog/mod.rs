//! The catalog command: one catalog's record and its schemas.
//!
//! The catalog is the entity above schemas. [`info`] reads its record;
//! [`ls`] lists the schemas in it.

pub mod info;
pub mod ls;
