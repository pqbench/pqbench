//! `partition ls`: the files a partition's commits added.
//!
//! [`list`] is the only public function. It returns plain data; the CLI owns
//! the document it becomes.

use std::collections::BTreeMap;
use std::fmt;

use crate::table::TableFile;

/// Errors listing a partition's files.
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

/// The files the given commits added, in log order.
///
/// A commit's `add` actions name the files it wrote; a file a later commit
/// removes is still named, so the result is the **added in the window** set,
/// not the table's active files. The size is left zero: the log's `add` size
/// is only a cross-check, and `bytemass` reads the true size from the footer.
///
/// # Errors
/// Fails when the table cannot be loaded.
pub async fn list(
    uri: &str,
    env: &BTreeMap<String, String>,
    versions: &[u64],
) -> Result<Vec<TableFile>, Error> {
    super::r#impl::list(uri, env, versions).await
}
