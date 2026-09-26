//! Third-party API wrappers, one folder per crate.
//!
//! Each wrapper splits into `api` (the isolated surface pqbench uses) and
//! `impl` (the only file that names the third-party crate).

pub mod avro;
pub mod chrono;
pub mod delta;
pub mod flate2;
pub mod iceberg;
pub mod lz4;
pub mod object_store;
pub mod parquet;
pub mod snappy;
pub mod unity;
pub mod zstd;
