//! The partition command: a table's natural partitions and the files they added.
//!
//! A partition is a grouping of a table's active files; the natural key is
//! time, so [`list::list`] groups the table's commits into epoch-aligned windows
//! and returns one `Partition` each. [`crate::file::list::list`] then reads the
//! files those commits named. The CLI owns the documents they become.

pub mod list;
