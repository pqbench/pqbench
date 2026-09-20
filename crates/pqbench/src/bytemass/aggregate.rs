//! Reducer: fold the measured table into per-column totals and the tree.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use crate::parquet_helpers::Error;

use super::analytics::{self, MassNode};
use super::api::MassRow;
use super::collection::{ColumnMassSummary, MassSummary};
use super::raw;

/// Sum the measured table across all files into a per-column summary.
///
/// # Errors
/// Returns [`Error`] if a row or byte total exceeds its integer type.
pub fn aggregate(rows: &[MassRow]) -> Result<MassSummary, Error> {
    let runs = file_runs(rows);
    let mut num_rows = 0;
    for run in &runs {
        num_rows = checked_sum(num_rows, run.num_rows)?;
    }
    let mut columns: BTreeMap<String, ColumnMassSummary> = BTreeMap::new();
    for row in rows {
        let total = columns
            .entry(row.column.clone())
            .or_insert_with(|| ColumnMassSummary {
                path: row.column.clone(),
                compressed_bytes: 0,
                uncompressed_bytes: 0,
                codecs: BTreeSet::new(),
            });
        total.compressed_bytes = checked_sum(total.compressed_bytes, row.compressed_bytes)?;
        total.uncompressed_bytes = checked_sum(total.uncompressed_bytes, row.uncompressed_bytes)?;
        total.codecs.insert(row.codec.clone());
    }
    Ok(MassSummary {
        file_count: runs.len(),
        num_rows,
        columns: columns.into_values().collect(),
    })
}

/// One entry per contiguous run of rows from the same file: the run's first row.
///
/// Files are emitted contiguously, so no row is dropped or merged to recover
/// the file boundaries.
fn file_runs(rows: &[MassRow]) -> Vec<&MassRow> {
    let mut runs: Vec<&MassRow> = Vec::new();
    for row in rows {
        if runs.last().is_none_or(|last| last.file != row.file) {
            runs.push(row);
        }
    }
    runs
}

/// Build the byte-mass tree that text, JSON, and d3 render.
pub(super) fn tree(rows: &[MassRow]) -> Result<MassNode, Error> {
    let summary = aggregate(rows)?;
    let mut tree = analytics::aggregate(&raw::read(&summary.file_mass()));
    tree.label = label(rows);
    Ok(tree)
}

/// The root label: one file's name, or `N parquet files` for a collection.
pub(super) fn label(rows: &[MassRow]) -> String {
    let files = file_runs(rows);
    match files.as_slice() {
        [] => "file".into(),
        [file] => Path::new(file.file.as_str())
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_else(|| "file".into()),
        files => format!("{} parquet files", files.len()),
    }
}

fn checked_sum(left: u64, right: u64) -> Result<u64, Error> {
    left.checked_add(right)
        .ok_or_else(|| Error("metadata totals exceed u64".into()))
}

#[cfg(test)]
mod tests {
    use super::super::api::MassRow;
    use super::*;

    fn row(file: &str, column: &str, codec: &str) -> MassRow {
        MassRow {
            file: file.into(),
            size: 1,
            num_rows: 3,
            column: column.into(),
            compressed_bytes: 12,
            uncompressed_bytes: 24,
            codec: codec.into(),
        }
    }

    #[test]
    fn aggregate_combines_files_and_column_codecs() {
        let rows = vec![row("a", "value", "SNAPPY"), row("b", "value", "SNAPPY")];
        let summary = aggregate(&rows).unwrap();
        assert_eq!(summary.file_count, 2);
        assert_eq!(summary.num_rows, 6);
        assert_eq!(summary.columns[0].compressed_bytes, 24);
        assert_eq!(summary.columns[0].uncompressed_bytes, 48);
        assert_eq!(summary.columns[0].codecs, BTreeSet::from(["SNAPPY".into()]));
        assert_eq!(summary.file_mass().columns[0].codec, "SNAPPY");
    }

    #[test]
    fn file_mass_preserves_multiple_codecs() {
        let rows = vec![row("a", "value", "ZSTD"), row("a", "value", "SNAPPY")];
        let mass = aggregate(&rows).unwrap().file_mass();
        assert_eq!(mass.columns[0].codec, "SNAPPY,ZSTD");
        assert_eq!(mass.num_rows, 3);
    }
}
