//! `partition::list`: a table's natural partitions.
//!
//! [`list`] is the only public function. It returns [`Partition`] resources;
//! the CLI forwards them as-is.

use std::fmt;

use futures_util::stream::{BoxStream, StreamExt};
use futures_util::TryStreamExt;

use crate::v1;
pub use crate::v1::Partition;

use crate::Lake;

/// Errors listing a table's partitions.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The window argument was invalid.
    Invalid(String),
    /// The table loader failed.
    Table(crate::table::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Invalid(message) => write!(f, "{message}"),
            Error::Table(_) => write!(f, "the table loader failed"),
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

/// A table's partitions, in window order, from [`list`].
pub type PartitionStream<'a> = BoxStream<'a, Result<Partition, Error>>;

/// Group a table's commits into natural partitions of `window_millis`.
///
/// Windows are half-open `[first, last)` and aligned to the Unix epoch, so a
/// daily window is a UTC day. A commit the log does not date is omitted, so a
/// window never claims a commit it cannot place.
///
/// Partitions are yielded as each window closes.
///
/// `last_millis` bounds the read to commits within that many milliseconds of
/// the newest dated commit; `None` reads every retained commit. The read stops
/// at the first commit out of reach, so a long log is not read to its oldest
/// retained commit, and only windows the horizon covers whole are yielded, so a
/// window never under-counts.
///
/// The table's resource name names each partition as its parent; its `uri` is
/// the storage root carried on each partition, so the files stage reads the log
/// without a catalog call, and its `credentials` are the lease each partition
/// carries. Each partition carries its commits' versions and, index-aligned
/// with them, the `commitInfo` the log stored for each — its operation,
/// parameters, metrics and writer — so a reader can select by operation without
/// re-reading the log.
///
/// # Errors
/// Fails when the table names no storage root, cannot be loaded, a window is
/// not positive, or the read window is not positive; an error raised after the
/// first partition arrives as a stream item.
pub async fn list(
    lake: &Lake,
    table: &v1::Table,
    window_millis: i64,
    last_millis: Option<i64>,
) -> Result<PartitionStream<'static>, Error> {
    let uri = table.uri.as_deref().ok_or_else(|| {
        Error::Invalid("a table record has no storage path; run `pqbench get table` first".into())
    })?;
    let env = table
        .credentials
        .as_ref()
        .map(v1::Credentials::to_env)
        .unwrap_or_default();
    let lease = table.credentials.clone().filter(|lease| !lease.is_empty());
    let partitions =
        super::r#impl::list(lake, &table.name, uri, &env, window_millis, last_millis).await?;
    Ok(partitions
        .map_ok(move |mut partition| {
            partition.credentials = lease.clone();
            partition
        })
        .boxed())
}
