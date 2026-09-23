//! Collect a bytemass row stream into SQLite and a static HTML page.
//!
//! `bytemass` measures files and writes `pqbench.bytemass-row` lines. This
//! module does not read Parquet. It stores those rows in SQLite and writes a
//! page whose JavaScript queries that database (sql.js) and draws a d3
//! treemap.

mod html;
mod sqlite;

use std::path::Path;

use crate::parquet_helpers::Error;

pub use html::render_html;
pub use sqlite::write_sqlite;

/// One collected bytemass row, tagged with the stream `id`.
#[derive(Debug, Clone, Default)]
pub struct MassRecord {
    /// Table id from the stream, or empty for a bare parquet input.
    pub id: String,
    /// Input path or URI as given.
    pub file: String,
    /// On-disk file size in bytes.
    pub size: u64,
    /// Number of rows in the file.
    pub num_rows: u64,
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
#[derive(Debug, Clone)]
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
    pub num_records: Option<u64>,
    /// Log size / num_records.
    pub bytes_per_row: Option<f64>,
    /// Storage class from HEAD, when known.
    pub storage_class: Option<String>,
    /// Hive partition values as `k=v/k=v`.
    pub partition: String,
}

/// Write `path.sqlite` and `path.html` from the collected rows.
///
/// # Errors
/// Fails when there are no rows, the SQLite file cannot be written, or the
/// HTML page cannot be rendered.
pub fn write_report(prefix: &Path, rows: &[MassRecord], files: &[FileMass]) -> Result<(), Error> {
    let sqlite = prefix.with_extension("sqlite");
    let html_path = prefix.with_extension("html");
    write_sqlite(&sqlite, rows, files)?;
    let bytes = std::fs::read(&sqlite)
        .map_err(|error| Error(format!("cannot read {}: {error}", sqlite.display())))?;
    let html = render_html(&bytes, &title(rows))?;
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
            num_rows: 2,
            column: column.into(),
            compressed_bytes: bytes,
            uncompressed_bytes: bytes,
            codec: "ZSTD".into(),
            ..MassRecord::default()
        }
    }

    #[test]
    fn report_writes_sqlite_and_html() {
        let directory = tempfile::tempdir().unwrap();
        let prefix = directory.path().join("masses");
        write_report(&prefix, &[row("", "a<b>.parquet", "text", 4)], &[]).unwrap();

        let sqlite = std::fs::read(prefix.with_extension("sqlite")).unwrap();
        assert!(sqlite.starts_with(b"SQLite format 3"));

        let html = std::fs::read_to_string(prefix.with_extension("html")).unwrap();
        assert!(html.starts_with("<!DOCTYPE html>"));
        assert!(html.contains("sql.js"));
        assert!(html.contains("d3-hierarchy@3"));
        assert!(html.contains("a&lt;b&gt;.parquet"));
        assert!(!html.contains("a<b>.parquet"));
    }

    #[test]
    fn sqlite_round_trips_rows() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("masses.sqlite");
        write_sqlite(
            &path,
            &[
                row("sales", "part-0.parquet", "id", 20),
                row("sales", "part-0.parquet", "sku", 8),
            ],
            &[],
        )
        .unwrap();
        let conn = rusqlite::Connection::open(&path).unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM masses", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 2);
        let column: String = conn
            .query_row(
                "SELECT column_path FROM masses WHERE compressed_bytes = 8",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(column, "sku");
    }

    #[test]
    fn sqlite_round_trips_file_stats() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("masses.sqlite");
        write_sqlite(
            &path,
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
        let conn = rusqlite::Connection::open(&path).unwrap();
        let bytes_per_row: f64 = conn
            .query_row(
                "SELECT bytes_per_row FROM files WHERE storage_class = 'STANDARD'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(bytes_per_row, 4.0);
    }

    #[test]
    fn empty_rows_fail() {
        let directory = tempfile::tempdir().unwrap();
        let error = write_sqlite(&directory.path().join("empty.sqlite"), &[], &[])
            .unwrap_err()
            .to_string();
        assert!(error.contains("no bytemass rows"), "{error}");
    }
}
