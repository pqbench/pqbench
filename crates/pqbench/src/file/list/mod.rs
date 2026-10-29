//! `file::list`: the active files of one table.
//!
//! [`api::visit`] is the public surface: it resolves the snapshot, walks its
//! active files, and hands each to the caller. Nothing is collected, so a
//! million-file table costs one batch plus one file.

pub mod api;
mod r#impl;

pub use api::{visit, Error};
