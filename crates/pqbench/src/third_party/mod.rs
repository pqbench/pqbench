//! Third-party API wrappers, one folder per crate.
//!
//! Each wrapper splits into `api` (the isolated surface pqbench uses) and
//! `impl` (the only file that names the third-party crate).
//!
//! The local-file wrappers (`parquet`, `snappy`, `zstd`, `lz4`, `flate2`) live
//! in `pqbench-parquet` and are re-exported here, so `pqbench::third_party::…`
//! names one wrapper either way. What stays is what only the network paths
//! need.

pub use pqbench_parquet::third_party::{flate2, lz4, parquet, snappy, zstd};

pub mod avro;
pub mod delta;
pub mod json;
pub mod object_store;
pub mod reqwest;
