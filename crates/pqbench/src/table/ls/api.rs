//! `table ls`: a table's natural partitions.
//!
//! [`list`] is the only public function. It returns plain data; the CLI owns
//! the document it becomes.

use std::collections::BTreeMap;
use std::fmt;

/// One natural partition: a half-open commit-time window and its commits.
#[derive(Debug, Clone)]
pub struct Partition {
    /// Window start, milliseconds since the Unix epoch (inclusive).
    pub first_time: i64,
    /// Window end, milliseconds since the Unix epoch (exclusive).
    pub last_time: i64,
    /// Commits in the window, in version order.
    pub commits: Vec<Commit>,
}

/// One commit in a partition.
#[derive(Debug, Clone)]
pub struct Commit {
    /// Commit version.
    pub version: u64,
    /// Commit time, milliseconds since the Unix epoch.
    pub commit_time: i64,
}

/// Errors listing a table's partitions.
#[derive(Debug)]
pub struct Error(String);

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "table: {}", self.0)
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
/// # Errors
/// Fails when the table cannot be loaded, or `window_millis` is not positive.
pub async fn list(
    uri: &str,
    env: &BTreeMap<String, String>,
    window_millis: i64,
) -> Result<Vec<Partition>, Error> {
    super::r#impl::list(uri, env, window_millis).await
}
