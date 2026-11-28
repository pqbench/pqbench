//! `bytemass`: the per-column byte masses of a Parquet blob.
//!
//! The fold — footer bytes in, one [`crate::v1::Column`] per leaf column out —
//! is local work, so it lives in [`pqbench_parquet::bytemass`] and this module
//! re-exports it. What is added here is the read: a HEAD for the storage class
//! and a bounded range GET for the footer.

mod api;
mod collection;
mod remote;

pub use api::{list_columns, measure_file, measure_local};
pub use pqbench_parquet::bytemass::{Error, MeasuredFile};
