//! pqbench is a benchmarking library for Parquet files (in-memory read/write
//! throughput, compression ratio), lzbench-style but Parquet-aware.

pub mod bench;
pub mod bytemass;
mod capability;
pub mod catalog;
pub mod codecs;
pub mod compression;
pub mod config;
pub mod credentials;
mod dialect;
pub mod diff;
pub mod experiment;
pub mod file;
pub mod http;
pub mod lz;
pub mod metastore;
pub mod partition;
pub mod profile;
pub mod ratelimit;
pub mod report;
pub mod schema;
pub mod skill;
pub mod stats;
pub mod table;
mod text;
pub mod third_party;
pub mod v1;
pub mod viz;

pub use capability::{Lake, Workspace};
pub use config::{Combine, Config, Dialect, Layer};
