//! pqbench-parquet: the local Parquet work — measure, compress, lint, profile.
//!
//! One file, no network. Everything here reads bytes that are already on the
//! machine: a footer, a page, a column chunk. The remote half of `pqbench`
//! (object stores, Delta, Iceberg, Unity, OAuth) is deliberately not a
//! dependency, so a change to a lint rule rebuilds this crate and its tests
//! alone — a few seconds, not the whole graph.
//!
//! The modules keep the names they have in `pqbench`, which re-exports them at
//! the same paths, so `pqbench::lint::…` and `pqbench_parquet::lint::…` are one
//! item.
//!
//! [`v1`] is the wire model, re-exported from `pqbench-model-v1` so the local
//! code reads `crate::v1::Column` exactly as it did before the split. It is an
//! alias of the same crate `pqbench` re-exports, never a second copy: a
//! `v1::Blob` from one is a `v1::Blob` for the other.

pub mod bench;
pub mod bytemass;
pub mod codecs;
pub mod compression;
pub mod diff;
pub mod lint;
pub mod lz;
pub mod mass;
pub mod price;
pub mod profile;
pub mod report;
pub mod stats;
pub mod text;
pub mod third_party;

pub use pqbench_model_v1 as v1;
