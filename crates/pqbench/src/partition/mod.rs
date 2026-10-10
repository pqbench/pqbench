//! `partition`: one natural partition and the files its commits added.
//!
//! [`ls::list`] re-reads the table's log and the snapshot's files, and returns
//! the files the given commits named. The CLI owns the document it becomes.

pub mod ls;
