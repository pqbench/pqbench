//! `file::list`: a table's active files, one at a time.
//!
//! [`list`] is the only public function. Each record names a file and carries
//! the table's lease, so a caller writes it as it arrives and holds nothing.

use std::fmt;

use futures_util::stream::BoxStream;
use futures_util::{StreamExt, TryStreamExt};

use crate::time::TimeWindow;
use crate::v1;
use crate::Lake;

/// A table's active files, newest first.
pub type FileStream = BoxStream<'static, Result<v1::TableFile, Error>>;

/// Errors listing a table's active files.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The table is not complete.
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

/// List a table's active files, newest write first in the log's own order.
///
/// The snapshot is resolved first — for Delta the log, its checkpoint, and the
/// commits after it — so the files are the ones active now, not the ones some
/// commit added. Files a later commit removed are absent.
///
/// `window` keeps only the files whose log records a modification time inside
/// it; the test runs before a file reaches the caller, so a daily run holds
/// the day's files rather than the table's. A file whose log records no time
/// cannot be shown to be in the window, so it is dropped.
///
/// Each file is named as `{table}/files/{path}` and carries the table's lease,
/// so a reader of the emitted record needs no catalog call.
///
/// # Errors
/// Fails when the table names no storage root or cannot be loaded. A failure
/// raised after the first file arrives as a stream item.
pub async fn list(
    lake: &Lake,
    table: &v1::Table,
    window: Option<TimeWindow>,
) -> Result<FileStream, Error> {
    let uri = table.uri.as_deref().ok_or_else(|| {
        Error::Invalid("a table record has no storage path; run `pqbench get table` first".into())
    })?;
    let env = table
        .credentials
        .as_ref()
        .map(v1::Credentials::to_env)
        .unwrap_or_default();
    let lease = table.credentials.clone().filter(|lease| !lease.is_empty());
    let name = table.name.clone();
    // The files are the snapshot's own active set, so the log is not read: the
    // record is the files, and carrying the commit history would only cost
    // memory.
    let request = crate::table::LoadRequest::new(uri.to_string(), None, env).without_log();
    let files = crate::table::stream_active_files(lake, &request)
        .await
        .map_err(Error::Table)?;
    Ok(files
        .map_err(Error::Table)
        .try_filter_map(move |file| {
            let record = if !window.is_none_or(|window| window.contains(file.modification_time)) {
                None
            } else {
                let mut file = file;
                file.name = v1::name::table_file_name(&name, &file.path);
                file.credentials = lease.clone();
                Some(file)
            };
            std::future::ready(Ok(record))
        })
        .boxed())
}
