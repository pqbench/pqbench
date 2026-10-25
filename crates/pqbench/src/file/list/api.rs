//! `file::list`: the files a partition's commits added.
//!
//! [`list`] is the only public function. It returns plain data; the CLI owns
//! the document it becomes.

use std::fmt;

use futures_util::stream::{BoxStream, StreamExt};
use futures_util::TryStreamExt;

use crate::v1;
use crate::v1::{Partition, TableFile};
use crate::Lake;

/// Errors listing a partition's files.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The table loader failed.
    Table(crate::table::Error),
    /// The partition is not complete.
    Invalid(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Table(_) => write!(f, "the table loader failed"),
            Error::Invalid(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Table(error) => Some(error),
            Error::Invalid(_) => None,
        }
    }
}

/// A partition's files, in log order, from [`list`].
pub type FileStream<'a> = BoxStream<'a, Result<TableFile, Error>>;

/// The files the partition's commits added, in log order.
///
/// The partition names its table, storage root, commits, and lease, so the
/// records are complete: each is `{table}/files/{path}` with the partition's
/// credentials, and `bytemass` reads it without a catalog call.
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
/// Fails when the partition names no table or storage root, or the table
/// cannot be loaded; an error raised after the first file arrives as a stream
/// item.
pub async fn list(lake: &Lake, partition: &Partition) -> Result<FileStream<'static>, Error> {
    let (catalog, schema, table, _) = Partition::parse_name(&partition.name).ok_or_else(|| {
        Error::Invalid(
            "a partition record needs a `/tables/{table}/partitions/{partition}` name".into(),
        )
    })?;
    let uri = partition
        .uri
        .as_deref()
        .ok_or_else(|| Error::Invalid("a partition record needs a table storage root".into()))?;
    let env = partition
        .credentials
        .as_ref()
        .map(v1::Credentials::to_env)
        .unwrap_or_default();
    let lease = partition
        .credentials
        .clone()
        .filter(|lease| !lease.is_empty());
    let files = super::r#impl::list(
        lake,
        &v1::name::table(&catalog, &schema, &table),
        uri,
        &env,
        &partition.commit_versions,
    )
    .await?;
    Ok(files
        .map_ok(move |mut file| {
            file.credentials = lease.clone();
            file
        })
        .boxed())
}
