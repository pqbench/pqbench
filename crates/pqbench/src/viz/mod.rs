//! Collect a bytemass row stream into a static HTML treemap page.
//!
//! `bytemass` measures files and writes `pqbench.bytemass-row` lines. This
//! module does not read Parquet. It embeds those rows in a page that groups
//! them by table id and draws a d3 treemap.

mod html;

use std::path::Path;

use crate::third_party::parquet::api::Error;

pub use html::render_html;

/// One collected bytemass row, tagged with the stream `id`.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct MassRecord {
    /// Table id from the stream, or empty for a bare parquet input.
    pub id: String,
    /// Input path or URI as given.
    pub file: String,
    /// On-disk file size in bytes.
    pub size: u64,
    /// Number of rows in the file.
    pub row_count: u64,
    /// Column path in schema form.
    pub column: String,
    /// On-disk (compressed) bytes for this column chunk.
    pub compressed_bytes: u64,
    /// Encoded bytes before compression.
    pub uncompressed_bytes: u64,
    /// Compression codec recorded in the column chunk metadata.
    pub codec: String,
    /// Encodings listed on the column chunk, comma-joined.
    pub encodings: String,
    /// Values in this chunk (including nulls).
    // aipnaming: allow(aip-141/count-suffix)
    pub num_values: u64,
    /// Dictionary page offset is present.
    pub dictionary: bool,
    /// Footer `null_count`, when statistics exist.
    pub null_count: Option<u64>,
    /// Footer `distinct_count`, when statistics exist.
    pub distinct_count: Option<u64>,
    /// Physical type of the leaf column.
    pub physical_type: String,
    /// Row-group index (0-based).
    pub row_group: u32,
    /// Rows in this row group.
    pub row_group_rows: u64,
    /// Compressed bytes / row-group rows.
    pub compressed_bytes_per_row: Option<f64>,
    /// Data pages in the OffsetIndex, when `--indexes` loaded one.
    pub page_count: Option<u64>,
}

/// One proxied table-file / object-stat row collected from the bytemass stream.
#[derive(Debug, Clone, serde::Serialize)]
pub struct FileMass {
    /// Table id from the stream.
    pub id: String,
    /// Table-relative path, when known.
    pub path: String,
    /// URI or filesystem path that was measured.
    pub file: String,
    /// Log or object size in bytes.
    pub size: u64,
    /// `numRecords` from Delta add stats.
    // aipnaming: allow(aip-141/count-suffix)
    pub num_records: Option<u64>,
    /// Log size / num_records.
    pub bytes_per_row: Option<f64>,
    /// Storage class from HEAD, when known.
    pub storage_class: Option<String>,
    /// Hive partition values as `k=v/k=v`.
    pub partition: String,
}

/// Write `path.html` from the collected rows and proxied file stats.
///
/// # Errors
/// Fails when there are no rows or the HTML page cannot be written.
pub fn write_report(prefix: &Path, rows: &[MassRecord], files: &[FileMass]) -> Result<(), Error> {
    let html_path = prefix.with_extension("html");
    let html = render_html(rows, files, &title(rows))?;
    std::fs::write(&html_path, html)
        .map_err(|error| Error(format!("cannot write {}: {error}", html_path.display())))
}

fn title(rows: &[MassRecord]) -> String {
    let mut ids = rows.iter().map(|row| row.id.as_str()).collect::<Vec<_>>();
    ids.sort_unstable();
    ids.dedup();
    match ids.as_slice() {
        [] => "bytemass".into(),
        [""] => file_name(&rows[0].file),
        [id] => (*id).into(),
        ids => format!("{} tables", ids.len()),
    }
}

fn file_name(path: &str) -> String {
    Path::new(path)
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "bytemass".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(id: &str, file: &str, column: &str, bytes: u64) -> MassRecord {
        MassRecord {
            id: id.into(),
            file: file.into(),
            size: 10,
            row_count: 2,
            column: column.into(),
            compressed_bytes: bytes,
            uncompressed_bytes: bytes,
            codec: "ZSTD".into(),
            ..MassRecord::default()
        }
    }

    #[test]
    fn report_writes_html() {
        let directory = tempfile::tempdir().unwrap();
        let prefix = directory.path().join("masses");
        write_report(&prefix, &[row("", "a<b>.parquet", "text", 4)], &[]).unwrap();

        let html = std::fs::read_to_string(prefix.with_extension("html")).unwrap();
        assert!(html.starts_with("<!DOCTYPE html>"));
        assert!(html.contains("d3-hierarchy@3"));
        assert!(html.contains("a&lt;b&gt;.parquet"));
        assert!(!html.contains("a<b>.parquet"));
        assert!(!html.contains("</script><script"));
    }

    #[test]
    fn report_embeds_proxied_file_stats() {
        let directory = tempfile::tempdir().unwrap();
        let prefix = directory.path().join("masses");
        write_report(
            &prefix,
            &[row("sales", "part-0.parquet", "id", 20)],
            &[FileMass {
                id: "sales".into(),
                path: "year=2024/part-0.parquet".into(),
                file: "part-0.parquet".into(),
                size: 40,
                num_records: Some(10),
                bytes_per_row: Some(4.0),
                storage_class: Some("STANDARD".into()),
                partition: "year=2024".into(),
            }],
        )
        .unwrap();
        let html = std::fs::read_to_string(prefix.with_extension("html")).unwrap();
        assert!(html.contains("STANDARD"), "{html}");
        assert!(html.contains("year=2024"), "{html}");
        assert!(html.contains("fileTree"), "{html}");
    }

    #[test]
    fn empty_rows_fail() {
        let directory = tempfile::tempdir().unwrap();
        let error = write_report(&directory.path().join("empty"), &[], &[])
            .unwrap_err()
            .to_string();
        assert!(error.contains("bytemass rows"), "{error}");
    }
}
