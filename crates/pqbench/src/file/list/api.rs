//! `file::list`: a table's active files, one at a time.
//!
//! [`visit`] is the only public function. It returns the version the files are
//! active at; the caller owns what each file becomes.

use std::fmt;

use crate::time::TimeWindow;
use crate::v1;
use crate::Lake;

/// Errors walking a table's active files.
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

/// Visit a table's active files, newest write first in the log's own order.
///
/// The snapshot is resolved first — for Delta the log, its checkpoint, and the
/// commits after it — so the files are the ones active now, not the ones some
/// commit added. Files a later commit removed are absent.
///
/// `window` keeps only the files whose log records a modification time inside
/// it; the test runs before a file reaches the callback, so a daily run holds
/// the day's files rather than the table's. A file whose log records no time
/// cannot be shown to be in the window, so it is dropped.
///
/// Each file is named as `{table}/files/{path}` and carries the table's lease,
/// so a reader of the emitted record needs no catalog call. `visit` returns
/// whether to continue; `false` ends the walk at that file.
///
/// # Errors
/// Fails when the table names no storage root or cannot be loaded. The version
/// the callback saw is returned on success.
pub async fn visit(
    lake: &Lake,
    table: &v1::Table,
    window: Option<TimeWindow>,
    visit: &mut (dyn FnMut(v1::TableFile) -> bool + Send),
) -> Result<u64, Error> {
    let uri = table.uri.as_deref().ok_or_else(|| {
        Error::Invalid("a table record has no storage path; run `pqbench get table` first".into())
    })?;
    let env = table
        .credentials
        .as_ref()
        .map(v1::Credentials::to_env)
        .unwrap_or_default();
    let lease = table.credentials.clone().filter(|lease| !lease.is_empty());
    super::r#impl::visit(lake, &table.name, uri, &env, lease, window, visit).await
}
