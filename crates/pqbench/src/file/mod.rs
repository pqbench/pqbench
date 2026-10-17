//! The file command: the files a partition's commits added.
//!
//! [`ls::list`] reads the table's log for the named commits' `add` actions and
//! streams the files they name, one `TableFile` each. The CLI owns the document
//! it becomes.

pub mod ls;
