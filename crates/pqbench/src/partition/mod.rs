//! The partition command: a table's snapshot, folded per partition.
//!
//! [`list::list`] reads the snapshot's active files once and keeps one small
//! accumulator per partition, never the files, so the cost in memory is the
//! number of partitions, not of files.

pub mod list;
pub mod price;
pub mod stats;
