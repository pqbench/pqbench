//! `file::list`: the active files of one table.
//!
//! [`api::list`] is the public surface: it resolves the snapshot and returns
//! its active files as a stream. Nothing is collected, so a million-file table
//! costs one log batch plus one file.

pub mod api;

pub use api::{list, Error, FileStream};
