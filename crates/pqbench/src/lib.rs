//! pqbench is a benchmarking library for Parquet files (in-memory read/write
//! throughput, compression ratio), lzbench-style but Parquet-aware.

pub mod bench;
pub mod bytemass;
pub mod codecs;
pub mod compression;
pub mod dump;
pub mod experiment;
pub mod lake;
pub mod lz;
mod object_store;
pub mod parquet_helpers;
mod parquet_impl;
pub mod pattern;
pub mod profile;
pub mod report;
pub mod skill;
pub mod stats;
pub mod table;
mod text;
pub mod viz;
