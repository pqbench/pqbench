//! `file::list`: the files a partition's commits added.
//!
//! [`list`] is the only public function. It returns plain data; the CLI owns
//! the document it becomes.

use std::collections::BTreeMap;
use std::fmt;

use futures_util::stream::BoxStream;

use crate::v1::TableFile;
use crate::Lake;

/// Errors listing a partition's files.
#[derive(Debug)]
pub enum Error {
    /// The table loader failed.
    Table(crate::table::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Table(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Table(error) => Some(error),
        }
    }
}

/// A partition's files, in log order, from [`list`].
pub type FileStream<'a> = BoxStream<'a, Result<TableFile, Error>>;

/// The files the given commits added, in log order.
///
/// `table` is the table's resource name: each file is named
/// `{table}/files/{path}`, so the record is complete without the caller
/// re-naming it. The caller, which holds the lease, sets each file's
/// `credentials`.
///
/// A commit's `add` actions name the files it wrote; a file a later commit
/// removes is still named, so the result is the **added in the window** set,
/// not the table's active files. The size is left zero: the log's `add` size
/// is only a cross-check, and `bytemass` reads the true size from the footer.
///
/// Files are yielded as each commit is read, so a reader writes the first
/// window's files while the rest of the log is still being read.
///
/// # Errors
/// Fails when the table cannot be loaded; an error raised after the first file
/// arrives as a stream item.
pub async fn list(
    lake: &Lake,
    table: &str,
    uri: &str,
    env: &BTreeMap<String, String>,
    versions: &[u64],
) -> Result<FileStream<'static>, Error> {
    super::r#impl::list(lake, table, uri, env, versions).await
}
