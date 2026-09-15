//! pqbench is a benchmarking library for Parquet files (in-memory read/write
//! throughput, compression ratio), lzbench-style but Parquet-aware.

pub mod bytemass;
pub mod codecs;
pub mod compression;
pub mod lz;
pub mod parquet_helpers;
mod parquet_impl;
pub mod report;
pub mod stats;
mod text;
