//! Local Delta snapshot storage analysis.
//!
//! delta-rs resolves the snapshot; only active data-file footers are inspected.
//! Results measure physical storage, not decoded values or logical live rows.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};

use deltalake::DeltaTableBuilder;
use futures::TryStreamExt;
use serde::Serialize;
use url::Url;

use pqbench::bytemass::{self, MassNode};
use pqbench::parquet_helpers::{default_metadata_parser, ColumnMass, FileMass, MetadataParser};

/// Errors resolving a local snapshot or measuring its active files.
#[derive(Debug)]
pub struct Error(String);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "delta: {}", self.0)
    }
}

impl std::error::Error for Error {}

/// A column's physical storage summed across all active data files.
#[derive(Serialize)]
pub struct ColumnReport {
    pub path: String,
    pub compressed_bytes: u64,
    pub uncompressed_bytes: u64,
    pub codecs: BTreeSet<String>,
}

/// A complete measurement of one local snapshot. File bytes include Parquet
/// overhead, but exclude the Delta log, tombstones, and unrelated files.
#[derive(Serialize)]
pub struct TableReport {
    pub version: u64,
    pub file_count: usize,
    pub physical_rows: u64,
    pub file_bytes: u64,
    pub compressed_column_bytes: u64,
    pub uncompressed_column_bytes: u64,
    /// Partition values live in the log and need not occupy Parquet columns.
    pub partition_columns: Vec<String>,
    pub columns: Vec<ColumnReport>,
    /// Compressed column bytes per physical table row, for JSON/HTML consumers.
    tree: MassNode,
}

impl TableReport {
    /// Total compressed column bytes per physical Parquet row.
    pub fn compressed_bytes_per_row(&self) -> f64 {
        self.tree.value
    }
}

/// Analyze the latest or requested version of a local Delta table.
///
/// Run inside a Tokio runtime. Paths are filesystem paths, not storage URIs.
///
/// # Errors
/// Fails for invalid snapshots, missing/changed active files, external data
/// paths, column mapping, deletion vectors, or unsupported Delta reader features.
/// No partial report is returned on failure.
pub async fn read_local(path: &Path, version: Option<u64>) -> Result<TableReport, Error> {
    let root = path
        .canonicalize()
        .map_err(|e| Error(format!("cannot open table {}: {e}", path.display())))?;
    if !root.join("_delta_log").is_dir() {
        return Err(Error(format!("missing _delta_log in {}", root.display())));
    }
    let url = Url::from_directory_path(&root)
        .map_err(|()| Error("cannot convert table path to a local file URL".into()))?;
    let mut builder = DeltaTableBuilder::from_url(url).map_err(delta_error)?;
    if let Some(version) = version {
        builder = builder.with_version(version);
    }
    let table = builder.load().await.map_err(delta_error)?;
    let snapshot = table.snapshot().map_err(delta_error)?;
    if snapshot
        .metadata()
        .configuration()
        .get("delta.columnMapping.mode")
        .is_some_and(|mode| mode != "none")
    {
        return Err(Error(
            "column mapping is not supported by local byte-mass analysis".into(),
        ));
    }

    let version = snapshot.version();
    let partition_columns = snapshot.metadata().partition_columns().to_vec();
    let mut files = table.get_active_add_actions_by_partitions(&[]);
    let parser = default_metadata_parser();
    let mut totals = BTreeMap::<String, ColumnReport>::new();
    let mut physical_rows = 0;
    let mut file_bytes = 0;
    let mut file_count = 0;
    while let Some(file) = files.try_next().await.map_err(delta_error)? {
        if file.deletion_vector_descriptor().is_some() {
            return Err(Error(
                "deletion vectors are not supported by local byte-mass analysis".into(),
            ));
        }
        let relative = file.path();
        let local = local_file(&root, relative.as_ref())?;
        let expected = u64::try_from(file.size())
            .map_err(|_| Error(format!("invalid file size in log: {relative}")))?;
        let actual = std::fs::metadata(&local)
            .map_err(|e| Error(format!("cannot stat active file {relative}: {e}")))?
            .len();
        if actual != expected {
            return Err(Error(format!("active file size differs from log: {relative} (expected {expected}, found {actual})")));
        }
        let mass = parser
            .read_masses(&local)
            .map_err(|e| Error(format!("cannot read active file {relative}: {e}")))?;
        physical_rows = checked_sum(physical_rows, mass.num_rows)?;
        file_bytes = checked_sum(file_bytes, actual)?;
        file_count += 1;
        for column in mass.columns {
            let total = totals
                .entry(column.path.clone())
                .or_insert_with(|| ColumnReport {
                    path: column.path,
                    compressed_bytes: 0,
                    uncompressed_bytes: 0,
                    codecs: BTreeSet::new(),
                });
            total.compressed_bytes = checked_sum(total.compressed_bytes, column.bytes)?;
            total.uncompressed_bytes =
                checked_sum(total.uncompressed_bytes, column.uncompressed_bytes)?;
            total.codecs.insert(column.codec);
        }
    }
    let columns: Vec<_> = totals.into_values().collect();
    let mut compressed_column_bytes = 0;
    let mut uncompressed_column_bytes = 0;
    for column in &columns {
        compressed_column_bytes = checked_sum(compressed_column_bytes, column.compressed_bytes)?;
        uncompressed_column_bytes =
            checked_sum(uncompressed_column_bytes, column.uncompressed_bytes)?;
    }
    let mass = FileMass {
        num_rows: physical_rows,
        columns: columns
            .iter()
            .map(|c| ColumnMass {
                path: c.path.clone(),
                bytes: c.compressed_bytes,
                uncompressed_bytes: c.uncompressed_bytes,
                codec: String::new(),
            })
            .collect(),
    };
    let mut tree = bytemass::aggregate(&bytemass::read(&mass));
    let label = root
        .file_name()
        .unwrap_or(root.as_os_str())
        .to_string_lossy();
    tree.label = format!("{label} @ version {version} (physical bytes/row)");
    Ok(TableReport {
        version,
        file_count,
        physical_rows,
        file_bytes,
        compressed_column_bytes,
        uncompressed_column_bytes,
        partition_columns,
        columns,
        tree,
    })
}

/// Serialize the snapshot report as pretty-printed JSON.
pub fn json(report: &TableReport) -> Result<String, Error> {
    serde_json::to_string_pretty(report).map_err(|e| Error(format!("cannot serialize report: {e}")))
}

/// Render the physical byte-mass hierarchy as a self-contained HTML treemap.
///
/// # Errors
/// Returns an error if the hierarchy cannot be serialized.
pub fn render_html(report: &TableReport) -> Result<String, Error> {
    bytemass::render_html(&report.tree)
        .map_err(|e| Error(format!("cannot render HTML report: {e}")))
}

/// Render the snapshot summary followed by the existing byte-mass table.
pub fn render(report: &TableReport) -> String {
    format!(
        "delta version: {}\nactive files: {}\nphysical rows: {}\nactive parquet bytes: {}\ncompressed column bytes: {}\nuncompressed column bytes: {}\n{}",
        report.version,
        report.file_count,
        report.physical_rows,
        report.file_bytes,
        report.compressed_column_bytes,
        report.uncompressed_column_bytes,
        bytemass::render(&report.tree),
    )
}

fn delta_error(error: deltalake::DeltaTableError) -> Error {
    Error(format!("cannot resolve snapshot: {error}"))
}

fn checked_sum(left: u64, right: u64) -> Result<u64, Error> {
    left.checked_add(right)
        .ok_or_else(|| Error("storage totals exceed u64".into()))
}

fn local_file(root: &Path, relative: &str) -> Result<PathBuf, Error> {
    let path = Path::new(relative);
    if relative.contains("://") || !path.components().all(|c| matches!(c, Component::Normal(_))) {
        return Err(Error(format!(
            "only relative data paths inside the table are supported: {relative}"
        )));
    }
    let local = root
        .join(path)
        .canonicalize()
        .map_err(|e| Error(format!("cannot open active file {relative}: {e}")))?;
    if !local.starts_with(root) {
        return Err(Error(format!(
            "active file is outside the table directory: {relative}"
        )));
    }
    Ok(local)
}
