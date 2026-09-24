//! Third-party API wrappers, one folder per crate.
//!
//! Each wrapper splits into `api` (the isolated surface pqbench uses) and
//! `impl` (the only file that names the third-party crate).

pub mod avro;
pub mod delta;
pub(crate) mod object_store;
pub mod parquet;
pub mod unity;
