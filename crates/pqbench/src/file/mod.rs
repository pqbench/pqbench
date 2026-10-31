//! The file command: a table's active files.
//!
//! [`list::list`] streams one snapshot's active files, so a caller that writes
//! each file as it arrives holds one file at a time. The CLI owns the document
//! it becomes.

pub mod list;
pub mod sample;
