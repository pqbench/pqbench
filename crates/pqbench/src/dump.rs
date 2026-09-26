//! Copy the Parquet files a table names to a local directory.
//!
//! `dump` is deliberately dumb: for each file it fetches the object — a local
//! path or a remote URI, through [`crate::third_party::object_store`] — and
//! writes it under the output directory at the file's table-relative path.
//! Selecting which files to keep is the shell's job on the `table` stream, so
//! there is no include/exclude/sample here.

use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};

use crate::third_party::object_store;

/// One file to copy: where it lands and where to read it.
#[derive(Debug, Clone)]
pub struct DumpFile {
    /// Table-relative path; the copy lands here under the output directory.
    pub path: String,
    /// URI or filesystem path to the Parquet object.
    pub uri: String,
    /// Storage options for this file (`AWS_*`).
    pub env: BTreeMap<String, String>,
}

/// What one [`put`] copied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DumpSummary {
    /// Files written.
    pub file_count: usize,
    /// Total bytes written.
    pub byte_count: u64,
}

/// Errors copying a table's files.
#[derive(Debug)]
pub struct Error(String);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "dump: {}", self.0)
    }
}

impl std::error::Error for Error {}

/// Copy `files` under `dir`, creating parent directories.
///
/// Each file's [`DumpFile::path`] is resolved under `dir`; a path that escapes
/// `dir` (absolute, `..`, or a drive prefix) is refused.
///
/// # Errors
/// Fails on an unsafe file path, or when an object cannot be read or written.
pub async fn put(files: &[DumpFile], dir: &Path) -> Result<DumpSummary, Error> {
    let mut summary = DumpSummary {
        file_count: 0,
        byte_count: 0,
    };
    for file in files {
        let dest = dir.join(relative_path(&file.path)?);
        let options: Vec<(String, String)> = file
            .env
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        let bytes = object_store::copy(&file.uri, &dest, &options)
            .await
            .map_err(|error| Error(error.to_string()))?;
        summary.file_count += 1;
        summary.byte_count += bytes;
    }
    Ok(summary)
}

/// Reject a path that could escape the output directory.
fn relative_path(path: &str) -> Result<PathBuf, Error> {
    let source = Path::new(path);
    if source.is_absolute() {
        return Err(Error(format!("refusing to write absolute path `{path}`")));
    }
    let mut relative = PathBuf::new();
    for component in source.components() {
        match component {
            Component::Normal(part) => relative.push(part),
            Component::CurDir => {}
            _ => return Err(Error(format!("refusing to write path `{path}`"))),
        }
    }
    if relative.as_os_str().is_empty() {
        return Err(Error(format!("refusing to write path `{path}`")));
    }
    Ok(relative)
}
