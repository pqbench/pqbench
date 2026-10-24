//! Collect bytemass columns into a static HTML treemap page.
//!
//! `bytemass` measures files and writes `v1::Blob` and `v1::Bytemass` lines.
//! This module does not read Parquet. It embeds those columns in a page that
//! draws a d3 treemap of on-disk bytes per value.

mod html;

use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;

use crate::v1;

pub use html::render_html;

/// Errors writing the treemap page.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// There were no columns to draw.
    Empty,
    /// The columns could not be encoded into the page.
    Render(serde_json::Error),
    /// The page could not be written.
    Write {
        /// The path that could not be written.
        path: String,
        /// The underlying I/O failure.
        source: std::io::Error,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Empty => write!(f, "html needs bytemass columns"),
            Error::Render(_) => write!(f, "cannot encode the columns"),
            Error::Write { path, .. } => write!(f, "cannot write {path}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Render(error) => Some(error),
            Error::Write { source, .. } => Some(source),
            Error::Empty => None,
        }
    }
}

/// One column's summed bytes, ready for the page.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct ColumnRow {
    /// Column path in schema form.
    pub column: String,
    /// On-disk (compressed) bytes over every file that measured it.
    pub compressed_bytes: u64,
    /// Values over every file that measured it.
    pub value_count: u64,
}

/// Write `path.html` from the measured columns and the blobs they came from.
///
/// # Errors
/// Fails when there are no columns or the HTML page cannot be written.
pub fn write_report(
    prefix: &Path,
    columns: &[v1::Bytemass],
    blobs: &[v1::Blob],
) -> Result<(), Error> {
    let rows = fold(columns);
    let html_path = prefix.with_extension("html");
    let html = render_html(&rows, blobs, &title(blobs))?;
    std::fs::write(&html_path, html).map_err(|error| Error::Write {
        path: html_path.display().to_string(),
        source: error,
    })
}

/// Sum each column's bytes and values over the files that measured it.
fn fold(columns: &[v1::Bytemass]) -> Vec<ColumnRow> {
    let mut at: BTreeMap<String, usize> = BTreeMap::new();
    let mut rows: Vec<ColumnRow> = Vec::new();
    for column in columns {
        let Some((_, path)) = v1::name::parse_bytemass(&column.name) else {
            continue;
        };
        let index = *at.entry(path.clone()).or_insert_with(|| {
            rows.push(ColumnRow {
                column: path.clone(),
                ..ColumnRow::default()
            });
            rows.len() - 1
        });
        let row = &mut rows[index];
        row.compressed_bytes += column.compressed_bytes.unwrap_or(0);
        row.value_count += column.value_count.unwrap_or(0);
    }
    rows
}

fn title(blobs: &[v1::Blob]) -> String {
    match blobs {
        [] => "bytemass".into(),
        [blob] => file_name(blob),
        blobs => format!("{} files", blobs.len()),
    }
}

/// The last locator segment of a blob name.
fn file_name(blob: &v1::Blob) -> String {
    let (_, locator) = v1::name::parse_blob(&blob.name).unwrap_or_default();
    locator
        .rsplit('/')
        .find(|segment| !segment.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| "bytemass".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn blob(locator: &str) -> v1::Blob {
        v1::Blob {
            name: v1::name::blob_name(locator),
            size_bytes: Some(40),
            storage_class: Some("STANDARD".into()),
            ..v1::Blob::default()
        }
    }

    fn column(blob: &v1::Blob, column: &str, bytes: u64) -> v1::Bytemass {
        v1::Bytemass {
            name: v1::name::bytemass(&blob.name, column),
            compressed_bytes: Some(bytes),
            value_count: Some(2),
            ..v1::Bytemass::default()
        }
    }

    #[test]
    fn report_writes_html() {
        let directory = tempfile::tempdir().unwrap();
        let prefix = directory.path().join("masses");
        let file = blob("/tmp/a<b>.parquet");
        write_report(&prefix, &[column(&file, "text", 4)], &[file]).unwrap();

        let html = std::fs::read_to_string(prefix.with_extension("html")).unwrap();
        assert!(html.starts_with("<!DOCTYPE html>"));
        assert!(html.contains("d3-hierarchy@3"));
        assert!(html.contains("a&lt;b&gt;.parquet"));
        assert!(!html.contains("a<b>.parquet"));
        assert!(!html.contains("</script><script"));
    }

    #[test]
    fn report_embeds_blob_facts() {
        let directory = tempfile::tempdir().unwrap();
        let prefix = directory.path().join("masses");
        let file = blob("/tmp/part-0.parquet");
        write_report(&prefix, &[column(&file, "id", 20)], &[file]).unwrap();

        let html = std::fs::read_to_string(prefix.with_extension("html")).unwrap();
        assert!(html.contains("STANDARD"), "{html}");
        assert!(html.contains("part-0.parquet"), "{html}");
        assert!(html.contains("fileTree"), "{html}");
    }

    #[test]
    fn empty_columns_fail() {
        let directory = tempfile::tempdir().unwrap();
        let error = write_report(&directory.path().join("empty"), &[], &[])
            .unwrap_err()
            .to_string();
        assert!(error.contains("bytemass columns"), "{error}");
    }
}
