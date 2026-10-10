//! `table ls`: a table's natural partitions, grouped by commit time.
//!
//! [`api`] is the public surface: one function, [`api::list`]. Partitions are
//! derived from the commit log — the commits' times, not the files — so the
//! cost is O(#commits), not O(#files). The wire document belongs to the CLI.

pub mod api;
mod r#impl;

pub use api::{list, Commit, Error, Partition};
