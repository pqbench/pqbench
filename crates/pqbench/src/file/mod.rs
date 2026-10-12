//! The file command: the files a partition's commits added.
//!
//! [`ls::list`] re-reads the table's log and the snapshot's files, and returns
//! the files the given commits named, one `TableFile` each. The CLI owns the
//! document it becomes.

pub mod ls;
