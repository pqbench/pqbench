//! Compare compressed column bytes from two bytemass measurements.

use crate::v1;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

/// Errors comparing two measurements.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The comparison input was invalid (bad depth, a duplicate column, or a
    /// name that is not a `Bytemass`).
    Invalid(String),
    /// A byte or value total exceeded `u64`.
    Overflow,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Invalid(message) => write!(f, "{message}"),
            Error::Overflow => write!(f, "a byte or value total overflowed u64"),
        }
    }
}

impl std::error::Error for Error {}

/// A per-column total, keyed by rolled-up column path.
type Totals = BTreeMap<String, u64>;

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
    pub left_values: u64,
    pub right_values: u64,
    pub left_bytes_per_value: Option<f64>,
    pub right_bytes_per_value: Option<f64>,
}

/// Compare full leaf paths, or dotted physical prefixes at a positive depth.
///
/// A column's bytes and values are summed over the files that measured it.
/// Equal totals do not establish equal contents; this measures size
/// differences, not value equivalence.
///
/// # Errors
/// Rejects depth zero, duplicate columns, non-`Bytemass` names, or overflow.
pub fn compare(
    left: &[v1::Bytemass],
    right: &[v1::Bytemass],
    depth: Option<usize>,
) -> Result<Vec<ColumnDelta>, Error> {
    if depth == Some(0) {
        return Err(Error::Invalid("depth must be positive".into()));
    }
    let (left, left_values) = summarize(left, depth)?;
    let (right, right_values) = summarize(right, depth)?;
    let paths: BTreeSet<_> = left.keys().chain(right.keys()).collect();
    Ok(paths
        .into_iter()
        .map(|column| {
            let left_bytes = left.get(column).copied();
            let right_bytes = right.get(column).copied();
            let delta_bytes =
                i128::from(right_bytes.unwrap_or(0)) - i128::from(left_bytes.unwrap_or(0));
            let left_values = left_values.get(column).copied().unwrap_or(0);
            let right_values = right_values.get(column).copied().unwrap_or(0);
            ColumnDelta {
                column: column.clone(),
                left_bytes,
                right_bytes,
                delta_bytes,
                change_percent: left_bytes
                    .filter(|n| *n != 0)
                    .map(|n| delta_bytes as f64 * 100.0 / n as f64),
                left_values,
                right_values,
                left_bytes_per_value: left_bytes
                    .filter(|_| left_values != 0)
                    .map(|n| n as f64 / left_values as f64),
                right_bytes_per_value: right_bytes
                    .filter(|_| right_values != 0)
                    .map(|n| n as f64 / right_values as f64),
            }
        })
        .collect())
}

/// Sum bytes and values per rolled-up column path.
fn summarize(rows: &[v1::Bytemass], depth: Option<usize>) -> Result<(Totals, Totals), Error> {
    let mut names = BTreeSet::new();
    let mut bytes = Totals::new();
    let mut values = Totals::new();
    for row in rows {
        let (_, column) = v1::name::parse_bytemass(&row.name)
            .ok_or_else(|| Error::Invalid(format!("not a bytemass name: {}", row.name)))?;
        if !names.insert(row.name.clone()) {
            return Err(Error::Invalid(format!("duplicate bytemass: {}", row.name)));
        }
        let path = depth.map_or_else(
            || column.clone(),
            |depth| column.split('.').take(depth).collect::<Vec<_>>().join("."),
        );
        let total = bytes.entry(path.clone()).or_default();
        *total = total
            .checked_add(row.compressed_bytes.unwrap_or(0))
            .ok_or(Error::Overflow)?;
        let total = values.entry(path).or_default();
        *total = total
            .checked_add(row.value_count.unwrap_or(0))
            .ok_or(Error::Overflow)?;
    }
    Ok((bytes, values))
}
