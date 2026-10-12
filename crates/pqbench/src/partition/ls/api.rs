//! `partition ls`: a table's natural partitions.
//!
//! [`list`] is the only public function. It returns [`Partition`] resources;
//! the CLI forwards them as-is.

use std::collections::BTreeMap;
use std::fmt;

pub use crate::v1::Partition;

use crate::Lake;

/// Errors listing a table's partitions.
#[derive(Debug)]
pub struct Error(String);

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "partition: {}", self.0)
    }
}

impl std::error::Error for Error {}

impl From<String> for Error {
    fn from(message: String) -> Self {
        Self(message)
    }
}

/// Group a table's commits into natural partitions of `window_millis`.
///
/// Windows are half-open `[first, last)` and aligned to the Unix epoch, so a
/// daily window is a UTC day. A commit the log does not date is omitted, so a
/// window never claims a commit it cannot place.
///
/// `table` is the table's resource name; the partition names it as its parent.
/// `uri` and `env` are the table's storage root and lease, carried on each
/// partition so the files stage reads the log without a catalog call.
///
/// # Errors
/// Fails when the table cannot be loaded, or `window_millis` is not positive.
pub async fn list(
    lake: &Lake,
    table: &str,
    uri: &str,
    env: &BTreeMap<String, String>,
    window_millis: i64,
) -> Result<Vec<Partition>, Error> {
    super::r#impl::list(lake, table, uri, env, window_millis).await
}
