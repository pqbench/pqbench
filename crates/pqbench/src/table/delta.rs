//! Local Delta snapshot storage analysis.
//!
//! delta-rs resolves the snapshot; only active data-file footers are inspected.
//! Results measure physical storage, not decoded values or logical live rows.

use std::collections::BTreeSet;
use std::path::{Component, Path, PathBuf};

use deltalake::{DeltaTable, DeltaTableBuilder};
use futures::TryStreamExt;
use serde::Serialize;
use url::Url;

use crate::bytemass::{self, MassAccumulator, MassNode, MassSummary};
use crate::parquet_helpers::{default_metadata_parser, MetadataParser};

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
    let root = local_root(path)?;
    let table = load_local_table(&root, version).await?;
    let snapshot = snapshot_info(&table)?;
    let measured = measure_active_files(&table, &root).await?;
    build_report(&root, snapshot, measured)
}

struct SnapshotInfo {
    version: u64,
    partition_columns: Vec<String>,
}

struct MeasuredFiles {
    file_bytes: u64,
    mass: MassSummary,
}

fn local_root(path: &Path) -> Result<PathBuf, Error> {
    let root = path
        .canonicalize()
        .map_err(|e| Error(format!("cannot open table {}: {e}", path.display())))?;
    if !root.join("_delta_log").is_dir() {
        return Err(Error(format!("missing _delta_log in {}", root.display())));
    }
    Ok(root)
}

async fn load_local_table(root: &Path, version: Option<u64>) -> Result<DeltaTable, Error> {
    let url = Url::from_directory_path(root)
        .map_err(|()| Error("cannot convert table path to a local file URL".into()))?;
    let mut builder = DeltaTableBuilder::from_url(url).map_err(delta_error)?;
    if let Some(version) = version {
        builder = builder.with_version(version);
    }
    builder.load().await.map_err(delta_error)
}

fn snapshot_info(table: &DeltaTable) -> Result<SnapshotInfo, Error> {
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
    Ok(SnapshotInfo {
        version: snapshot.version(),
        partition_columns: snapshot.metadata().partition_columns().to_vec(),
    })
}

async fn measure_active_files(table: &DeltaTable, root: &Path) -> Result<MeasuredFiles, Error> {
    let mut files = table.get_active_add_actions_by_partitions(&[]);
    let parser = default_metadata_parser();
    let mut mass = MassAccumulator::new();
    let mut file_bytes = 0;
    while let Some(file) = files.try_next().await.map_err(delta_error)? {
        if file.deletion_vector_descriptor().is_some() {
            return Err(Error(
                "deletion vectors are not supported by local byte-mass analysis".into(),
            ));
        }
        let relative = file.path();
        let expected = u64::try_from(file.size())
            .map_err(|_| Error(format!("invalid file size in log: {relative}")))?;
        let (actual, file_mass) = measure_local_file(root, relative.as_ref(), expected, &parser)?;
        file_bytes = checked_sum(file_bytes, actual)?;
        mass.add(file_mass).map_err(parquet_error)?;
    }
    Ok(MeasuredFiles {
        file_bytes,
        mass: mass.finish(),
    })
}

fn measure_local_file(
    root: &Path,
    relative: &str,
    expected: u64,
    parser: &impl MetadataParser,
) -> Result<(u64, crate::parquet_helpers::FileMass), Error> {
    let local = local_file(root, relative)?;
    let actual = std::fs::metadata(&local)
        .map_err(|e| Error(format!("cannot stat active file {relative}: {e}")))?
        .len();
    if actual != expected {
        return Err(Error(format!(
            "active file size differs from log: {relative} (expected {expected}, found {actual})"
        )));
    }
    let mass = parser
        .read_masses(&local)
        .map_err(|e| Error(format!("cannot read active file {relative}: {e}")))?;
    Ok((actual, mass))
}

fn build_report(
    root: &Path,
    snapshot: SnapshotInfo,
    measured: MeasuredFiles,
) -> Result<TableReport, Error> {
    let mass = measured.mass.file_mass();
    let mut tree = bytemass::aggregate(&bytemass::read(&mass));
    let label = root
        .file_name()
        .unwrap_or(root.as_os_str())
        .to_string_lossy();
    tree.label = format!(
        "{label} @ version {} (physical bytes/row)",
        snapshot.version
    );
    let MassSummary {
        file_count,
        num_rows,
        columns,
    } = measured.mass;
    let columns: Vec<_> = columns
        .into_iter()
        .map(|column| ColumnReport {
            path: column.path,
            compressed_bytes: column.compressed_bytes,
            uncompressed_bytes: column.uncompressed_bytes,
            codecs: column.codecs,
        })
        .collect();
    let mut compressed_column_bytes = 0;
    let mut uncompressed_column_bytes = 0;
    for column in &columns {
        compressed_column_bytes = checked_sum(compressed_column_bytes, column.compressed_bytes)?;
        uncompressed_column_bytes =
            checked_sum(uncompressed_column_bytes, column.uncompressed_bytes)?;
    }
    Ok(TableReport {
        version: snapshot.version,
        file_count,
        physical_rows: num_rows,
        file_bytes: measured.file_bytes,
        compressed_column_bytes,
        uncompressed_column_bytes,
        partition_columns: snapshot.partition_columns,
        columns,
        tree,
    })
}

fn parquet_error(error: crate::parquet_helpers::Error) -> Error {
    Error(error.to_string())
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
