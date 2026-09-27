//! The schema command: one schema's record and the tables in it.
//!
//! The schema is the entity between catalogs and tables. [`info`] reads its
//! record; [`ls`] lists the tables in it as `pqbench.table-ref` lines.

mod dialect;
pub mod info;
pub mod ls;
