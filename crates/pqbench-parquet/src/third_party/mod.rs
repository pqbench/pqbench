//! Third-party API wrappers, one folder per crate.
//!
//! Each wrapper splits into `api` (the isolated surface pqbench uses) and
//! `impl` (the only file that names the third-party crate).
//!
//! These five are the ones a local file needs: Parquet itself, and the codecs
//! a page can be written with. The network wrappers (`object_store`,
//! `reqwest`, `delta`, `avro`, `json`) stay in `pqbench`.

pub mod flate2;
pub mod lz4;
pub mod parquet;
pub mod snappy;
pub mod zstd;
