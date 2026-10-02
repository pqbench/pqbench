//! Compare compressed column bytes from two bytemass measurements.

use crate::bytemass::MassRow;
use crate::third_party::parquet::api::Error;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

/// Byte totals and change for a physical column path or prefix.
#[derive(Debug, Clone, Serialize)]
pub struct ColumnDelta {
    pub column: String,
    pub left_bytes: Option<u64>,
    pub right_bytes: Option<u64>,
    /// Right minus left; absent columns contribute zero.
    pub delta_bytes: i128,
    /// Percentage change, absent for an absent or zero left baseline.
    pub change_percent: Option<f64>,
    pub left_rows: u64,
    pub right_rows: u64,
    pub left_bytes_per_row: Option<f64>,
    pub right_bytes_per_row: Option<f64>,
}

/// Compare full leaf paths, or dotted physical prefixes at a positive depth.
///
/// Row counts are counted once per file. Equal row counts do not establish
/// equal contents; this measures size differences, not value equivalence.
///
/// # Errors
/// Rejects depth zero, duplicate chunks, inconsistent row counts, or overflow.
pub fn compare(
    left: &[MassRow],
    right: &[MassRow],
    depth: Option<usize>,
) -> Result<Vec<ColumnDelta>, Error> {
    if depth == Some(0) {
        return Err(Error("depth must be positive".into()));
    }
    let (left, left_rows) = summarize(left, depth)?;
    let (right, right_rows) = summarize(right, depth)?;
    let paths: BTreeSet<_> = left.keys().chain(right.keys()).collect();
    Ok(paths
        .into_iter()
        .map(|column| {
            let left_bytes = left.get(column).copied();
            let right_bytes = right.get(column).copied();
            let delta_bytes =
                i128::from(right_bytes.unwrap_or(0)) - i128::from(left_bytes.unwrap_or(0));
            ColumnDelta {
                column: column.clone(),
                left_bytes,
                right_bytes,
                delta_bytes,
                change_percent: left_bytes
                    .filter(|n| *n != 0)
                    .map(|n| delta_bytes as f64 * 100.0 / n as f64),
                left_rows,
                right_rows,
                left_bytes_per_row: left_bytes
                    .filter(|_| left_rows != 0)
                    .map(|n| n as f64 / left_rows as f64),
                right_bytes_per_row: right_bytes
                    .filter(|_| right_rows != 0)
                    .map(|n| n as f64 / right_rows as f64),
            }
        })
        .collect())
}

fn summarize(
    rows: &[MassRow],
    depth: Option<usize>,
) -> Result<(BTreeMap<String, u64>, u64), Error> {
    let mut files = BTreeMap::new();
    let mut chunks = BTreeSet::new();
    let mut totals = BTreeMap::<String, u64>::new();
    for row in rows {
        if !chunks.insert((&row.uri, row.row_group, &row.column)) {
            return Err(Error(format!(
                "duplicate column chunk: {} {} {}",
                row.uri, row.row_group, row.column
            )));
        }
        if files
            .insert(&row.uri, row.row_count)
            .is_some_and(|count| count != row.row_count)
        {
            return Err(Error(format!("inconsistent row count: {}", row.uri)));
        }
        let path = depth.map_or_else(
            || row.column.clone(),
            |depth| {
                row.column
                    .split('.')
                    .take(depth)
                    .collect::<Vec<_>>()
                    .join(".")
            },
        );
        let total = totals.entry(path).or_default();
        *total = total
            .checked_add(row.compressed_bytes)
            .ok_or_else(|| Error("column byte total overflow".into()))?;
    }
    let rows = files.values().try_fold(0u64, |sum, rows| {
        sum.checked_add(*rows)
            .ok_or_else(|| Error("row total overflow".into()))
    })?;
    Ok((totals, rows))
}
