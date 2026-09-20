//! Delta snapshot storage analysis.
//!
//! The command is one function: [`delta`] takes a [`DeltaRequest`] and returns
//! the [`TableReport`]. Rendering is a fold of that report: [`render_text`]
//! prints the summary and byte-mass table, [`render_json`] serializes it, and
//! [`render_html`] wraps the byte-mass hierarchy in a self-contained treemap.
//!
//! delta-rs resolves the snapshot; only active data-file footers are inspected.
//! Results measure physical storage, not decoded values or logical live rows.

use std::collections::BTreeSet;
use std::path::{Component, Path, PathBuf};

use deltalake::{DeltaTable, DeltaTableBuilder};
use futures::TryStreamExt;
use serde::Serialize;
use url::Url;

use crate::bytemass::{
    self, aggregate, BytemassRequest, ColumnMassSummary, FileMassCache, MassRow, MassSummary,
};

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
#[derive(Debug, Clone, Serialize)]
#[non_exhaustive]
pub struct ColumnReport {
    /// Column path in schema form, e.g. `content` or `a.b`.
    pub path: String,
    /// Total on-disk bytes across the snapshot's active files.
    pub compressed_bytes: u64,
    /// Total encoded bytes before compression.
    pub uncompressed_bytes: u64,
    /// Compression codecs present in the active files.
    pub codecs: BTreeSet<String>,
}

/// A complete measurement of one local snapshot. File bytes include Parquet
/// overhead, but exclude the Delta log, tombstones, and unrelated files.
#[derive(Debug, Clone, Serialize)]
#[non_exhaustive]
pub struct TableReport {
    /// Resolved Delta snapshot version.
    pub version: u64,
    /// Number of active Parquet files.
    pub file_count: usize,
    /// Total physical rows in the active files.
    pub physical_rows: u64,
    /// Total size of the active Parquet files, including file overhead.
    pub file_bytes: u64,
    /// Total compressed column-chunk bytes.
    pub compressed_column_bytes: u64,
    /// Total uncompressed column-chunk bytes.
    pub uncompressed_column_bytes: u64,
    /// Partition values live in the log and need not occupy Parquet columns.
    pub partition_columns: Vec<String>,
    /// Per-column physical storage totals.
    pub columns: Vec<ColumnReport>,
    /// The measured table the report was folded from; kept for re-rendering.
    #[serde(skip)]
    rows: Vec<MassRow>,
}

impl TableReport {
    /// Total compressed column bytes per physical Parquet row.
    pub fn compressed_bytes_per_row(&self) -> f64 {
        if self.physical_rows == 0 {
            0.0
        } else {
            self.compressed_column_bytes as f64 / self.physical_rows as f64
        }
    }
}

/// Arguments for the `delta` command.
#[derive(Debug, Clone)]
pub struct DeltaRequest {
    /// Local table directory or table URI (`file://`, `s3://`, ...).
    pub table: String,
    /// Snapshot version; `None` selects the latest.
    pub version: Option<u64>,
}

/// Analyze the latest or requested version of a Delta table.
///
/// `request.table` is a filesystem path or a storage URI. Bare paths and
/// `file://` URIs are read through the filesystem; other schemes are resolved
/// by delta-rs (S3 requires the `delta-s3` feature). Run inside a Tokio
/// runtime.
///
/// # Errors
/// Fails for invalid snapshots, missing/changed active files, external data
/// paths, column mapping, deletion vectors, or unsupported Delta reader
/// features. No partial report is returned on failure.
pub async fn delta(request: &DeltaRequest) -> Result<TableReport, Error> {
    analyze(request, None).await
}

/// Analyze a snapshot, reusing Parquet file masses when the object is unchanged.
///
/// The cache is the Parquet-layer store: a later snapshot only footer-reads
/// files whose URI, size, or S3 ETag differ from a previous measurement.
///
/// # Errors
/// As [`delta`]. Also fails if a cache document cannot be written.
pub async fn delta_with_cache(
    request: &DeltaRequest,
    cache: &FileMassCache,
) -> Result<TableReport, Error> {
    analyze(request, Some(cache)).await
}

async fn analyze(
    request: &DeltaRequest,
    cache: Option<&FileMassCache>,
) -> Result<TableReport, Error> {
    if request.table.contains("://") {
        read_remote(&request.table, request.version, cache).await
    } else {
        read_local(Path::new(&request.table), request.version, cache).await
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
async fn read_local(
    path: &Path,
    version: Option<u64>,
    cache: Option<&FileMassCache>,
) -> Result<TableReport, Error> {
    let root = local_root(path)?;
    let table = load_local_table(&root, version).await?;
    read_table(&table, cache).await
}

/// Analyze the latest or requested version of a Delta table at a storage URI.
///
/// The URI is resolved by delta-rs. Active Parquet objects are measured through
/// `bytemass`'s public reader, which fetches object metadata and bounded footer
/// reads only — never data pages.
///
/// # Errors
/// Fails for invalid snapshots, missing or changed active objects, external
/// data paths, column mapping, deletion vectors, or unsupported Delta reader
/// features. No partial report is returned on failure.
async fn read_remote(
    uri: &str,
    version: Option<u64>,
    cache: Option<&FileMassCache>,
) -> Result<TableReport, Error> {
    let url = Url::parse(uri).map_err(|e| Error(format!("invalid table URI: {e}")))?;
    if url.scheme() == "file" {
        let path = url
            .to_file_path()
            .map_err(|()| Error("invalid local table URI".into()))?;
        return read_local(&path, version, cache).await;
    }
    let table = load_table(url, version).await?;
    read_table(&table, cache).await
}

async fn read_table(
    table: &DeltaTable,
    cache: Option<&FileMassCache>,
) -> Result<TableReport, Error> {
    let snapshot = snapshot_info(table)?;
    let active = active_files(table).await?;
    let (rows, file_bytes) = measure_active(&active, cache).await?;
    build_report(snapshot, file_bytes, rows)
}

struct SnapshotInfo {
    version: u64,
    partition_columns: Vec<String>,
}

/// One active data file: the input bytemass measures, its log path, and the
/// size the transaction log claims it has.
struct ActiveFile {
    input: String,
    relative: String,
    expected: u64,
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
    load_table(url, version).await
}

async fn load_table(url: Url, version: Option<u64>) -> Result<DeltaTable, Error> {
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

/// Resolve every active data file to the input `bytemass` measures, rejecting
/// deletion vectors and data paths outside the table.
async fn active_files(table: &DeltaTable) -> Result<Vec<ActiveFile>, Error> {
    let root = if table.table_url().scheme() == "file" {
        Some(
            table
                .table_url()
                .to_file_path()
                .map_err(|()| Error("invalid local table URI".into()))?,
        )
    } else {
        None
    };
    let mut files = table.get_active_add_actions_by_partitions(&[]);
    let mut active = vec![];
    while let Some(file) = files.try_next().await.map_err(delta_error)? {
        if file.deletion_vector_descriptor().is_some() {
            return Err(Error(
                "deletion vectors are not supported by byte-mass analysis".into(),
            ));
        }
        let relative = file.path().to_string();
        let expected = u64::try_from(file.size())
            .map_err(|_| Error(format!("invalid file size in log: {relative}")))?;
        let input = match &root {
            Some(root) => local_input(root, &relative, expected)?,
            None => object_uri(table.table_url(), &relative)?,
        };
        active.push(ActiveFile {
            input,
            relative,
            expected,
        });
    }
    Ok(active)
}

/// Measure the active files through `bytemass`, confirming each file's size
/// still matches the transaction log.
async fn measure_active(
    active: &[ActiveFile],
    cache: Option<&FileMassCache>,
) -> Result<(Vec<MassRow>, u64), Error> {
    if active.is_empty() {
        return Ok((vec![], 0));
    }
    let request = BytemassRequest {
        inputs: active.iter().map(|file| file.input.clone()).collect(),
    };
    let rows = match cache {
        Some(cache) => bytemass::bytemass_with_cache(&request, cache).await,
        None => bytemass::bytemass(&request).await,
    }
    .map_err(|e| Error(format!("cannot read active files: {e}")))?;
    let file_bytes = verify_sizes(active, &rows)?;
    Ok((rows, file_bytes))
}

fn verify_sizes(active: &[ActiveFile], rows: &[MassRow]) -> Result<u64, Error> {
    for row in rows {
        let file = active
            .iter()
            .find(|file| file.input == row.file)
            .ok_or_else(|| Error(format!("unexpected measured file: {}", row.file)))?;
        if row.size != file.expected {
            return Err(Error(format!(
                "active file size differs from log: {} (expected {}, found {})",
                file.relative, file.expected, row.size
            )));
        }
    }

    let mut file_bytes = 0;
    for file in active {
        file_bytes = checked_sum(file_bytes, file.expected)?;
    }
    Ok(file_bytes)
}

/// A data path that stays inside the table: empty, absolute, or URI paths are
/// rejected here, once, for both local and object adapters.
fn relative_data_path(relative: &str) -> Result<&Path, Error> {
    let path = Path::new(relative);
    if relative.is_empty()
        || relative.contains("://")
        || !path.components().all(|c| matches!(c, Component::Normal(_)))
    {
        return Err(Error(format!(
            "only relative data paths inside the table are supported: {relative}"
        )));
    }
    Ok(path)
}

/// Build a full object URI for one active file from the table URL.
fn object_uri(base: &Url, relative: &str) -> Result<String, Error> {
    relative_data_path(relative)?;
    let mut directory = base.clone();
    if !directory.path().ends_with('/') {
        directory.set_path(&format!("{}/", directory.path()));
    }
    Ok(directory
        .join(relative)
        .map_err(|e| Error(format!("invalid active file path {relative}: {e}")))?
        .into())
}

/// Resolve a local active file, rejecting escapes from the table directory.
fn local_file(root: &Path, relative: &str) -> Result<PathBuf, Error> {
    let path = relative_data_path(relative)?;
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

/// Resolve a local active file, rejecting escapes from the table directory and
/// early-detecting a size change before its footer is parsed.
fn local_input(root: &Path, relative: &str, expected: u64) -> Result<String, Error> {
    let local = local_file(root, relative)?;
    let size = std::fs::metadata(&local)
        .map_err(|e| Error(format!("cannot open active file {relative}: {e}")))?
        .len();
    if size != expected {
        return Err(Error(format!(
            "active file size differs from log: {relative} (expected {expected}, found {size})"
        )));
    }
    Ok(local.to_string_lossy().into_owned())
}

fn build_report(
    snapshot: SnapshotInfo,
    file_bytes: u64,
    rows: Vec<MassRow>,
) -> Result<TableReport, Error> {
    let summary = aggregate(&rows).map_err(|e| Error(e.to_string()))?;
    let MassSummary {
        file_count,
        num_rows,
        columns,
    } = summary;
    let columns = into_report_columns(columns);
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
        file_bytes,
        compressed_column_bytes,
        uncompressed_column_bytes,
        partition_columns: snapshot.partition_columns,
        columns,
        rows,
    })
}

fn into_report_columns(columns: Vec<ColumnMassSummary>) -> Vec<ColumnReport> {
    columns
        .into_iter()
        .map(|column| ColumnReport {
            path: column.path,
            compressed_bytes: column.compressed_bytes,
            uncompressed_bytes: column.uncompressed_bytes,
            codecs: column.codecs,
        })
        .collect()
}

fn delta_error(error: deltalake::DeltaTableError) -> Error {
    Error(format!("cannot resolve snapshot: {error}"))
}

fn checked_sum(left: u64, right: u64) -> Result<u64, Error> {
    left.checked_add(right)
        .ok_or_else(|| Error("storage totals exceed u64".into()))
}

/// Serialize the snapshot report as pretty-printed JSON.
pub fn render_json(report: &TableReport) -> Result<String, Error> {
    serde_json::to_string_pretty(report).map_err(|e| Error(format!("cannot serialize report: {e}")))
}

/// Render the physical byte-mass hierarchy as a self-contained HTML treemap.
///
/// # Errors
/// Returns an error if the hierarchy cannot be aggregated or serialized.
pub fn render_html(report: &TableReport) -> Result<String, Error> {
    bytemass::render_html(&report.rows).map_err(|e| Error(e.to_string()))
}

/// Render the snapshot summary followed by the byte-mass table.
///
/// # Errors
/// Returns an error if a byte total overflows while aggregating.
pub fn render_text(report: &TableReport) -> Result<String, Error> {
    let table = bytemass::render_text(&report.rows).map_err(|e| Error(e.to_string()))?;
    Ok(format!(
        "delta version: {}\nactive files: {}\nphysical rows: {}\nactive parquet bytes: {}\ncompressed column bytes: {}\nuncompressed column bytes: {}\n{}",
        report.version,
        report.file_count,
        report.physical_rows,
        report.file_bytes,
        report.compressed_column_bytes,
        report.uncompressed_column_bytes,
        table,
    ))
}
