//! The file command: a table's active files.
//!
//! [`list::visit`] walks one snapshot's active files and hands each to a
//! callback, so a caller that writes each file as it arrives holds one file at
//! a time. The CLI owns the document it becomes.

pub mod list;
