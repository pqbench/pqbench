//! pqbench is a benchmarking library for Parquet files (in-memory read/write
//! throughput, compression ratio), lzbench-style but Parquet-aware.
//!
//! The library is three layers, one crate each:
//!
//! - [`v1`] — the wire model (`pqbench-model-v1`): resources, their names,
//!   their JSON. No I/O, no third-party crate beyond `serde`.
//! - `pqbench-parquet` — the local work: measure a footer (`bytemass`),
//!   compress pages (`compression`), lint a column (`lint`), profile a sample
//!   (`profile`). No network.
//! - this crate — the remote half (object stores, Delta, Iceberg, Unity,
//!   OAuth) and the commands that walk it.
//!
//! The local modules are re-exported at the paths they had before the split,
//! so `pqbench::lint::suggestion` and `pqbench_parquet::lint::suggestion` are
//! one item and no caller had to move.

extern crate self as pqbench;

pub use pqbench_model_v1 as v1;
pub(crate) use pqbench_parquet::price;
pub use pqbench_parquet::{
    bench, codecs, compression, diff, lint, lz, mass, profile, report, stats,
};

pub mod blob;
pub mod bytemass;
mod capability;
pub mod catalog;
pub mod config;
pub mod credentials;
mod dialect;
pub mod file;
pub mod http;
pub mod metastore;
pub mod oauth;
pub mod partition;
pub mod schema;
pub mod skill;
pub mod table;
pub mod third_party;
pub mod time;
pub mod viz;

pub use capability::{Lake, Workspace};
pub use config::{Combine, Config, Dialect, Layer, Secret};
