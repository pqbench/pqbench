//! `partition ls`: the files a partition's commits added.
//!
//! [`api::list`] is the public surface. It reads the table's log and the
//! snapshot's files, and returns plain data; the CLI owns the document.

pub mod api;
mod r#impl;

pub use api::{list, Error};
