//! Every `pqbench` integration test, as one Cargo test target.
//!
//! Cargo gives each `tests/*.rs` file its own target, and each target statically
//! links the whole dependency graph (arrow, parquet, deltalake, the AWS SDK).
//! One target links that graph once instead of once per file, which is what
//! keeps `make check` fast. `autotests = false` in `Cargo.toml` stops Cargo
//! from also building the individual files as targets.
//!
//! Run one file's tests with a module filter, for example
//! `cargo test -p pqbench --all-features --test it -- sampling`.

mod common;
mod support;

#[path = "bytemass_e2e.rs"]
mod bytemass_e2e;

#[path = "compression_e2e.rs"]
mod compression_e2e;

#[path = "delta.rs"]
mod delta;

#[path = "diff.rs"]
mod diff;

#[path = "file_metadata.rs"]
mod file_metadata;

#[path = "file_selection.rs"]
mod file_selection;

#[path = "iceberg.rs"]
mod iceberg;

#[path = "lz_e2e.rs"]
mod lz_e2e;

#[path = "object_patterns.rs"]
mod object_patterns;

#[path = "redset_e2e.rs"]
mod redset_e2e;

#[path = "sampling.rs"]
mod sampling;
