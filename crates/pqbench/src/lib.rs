//! pqbench is a benchmarking library for Parquet files (in-memory read/write
//! throughput, compression ratio), lzbench-style but Parquet-aware.

pub mod bench;
pub mod bytemass;
pub mod codecs;
pub mod compression;
pub mod dump;
pub mod filter;
pub mod lake;
pub mod lz;
pub mod report;
pub mod stats;
pub mod table;
mod text;
pub mod third_party;
pub mod viz;
