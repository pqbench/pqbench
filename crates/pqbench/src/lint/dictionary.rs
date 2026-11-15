//! `dictionary-unique`: a dictionary page on nearly-unique values priced
//! without it.

use std::collections::HashSet;

use crate::third_party::parquet::api::Value;

use super::price::price_bytes;
use super::{ColumnSample, Error, Suggestion};

/// The rule id a suggestion carries.
pub const DICTIONARY_UNIQUE_RULE: &str = "dictionary-unique";

/// Values are nearly unique when at least this fraction of the sampled
/// non-null values are distinct.
const UNIQUE_FRACTION: f64 = 0.98;

/// The suggestion this column earns when its chunk carries a dictionary page
/// and the same sample without it prices smaller.
pub(super) fn suggest(sample: &ColumnSample<'_>) -> Result<Option<Suggestion>, Error> {
    if !sample.mass.dictionary || unique_fraction(&sample.cells) < UNIQUE_FRACTION {
        return Ok(None);
    }
    let rows: Vec<Vec<Value>> = sample
        .cells
        .iter()
        .map(|cell| {
            vec![match *cell {
                Some(bytes) => Value::Bytes(bytes.to_vec()),
                None => Value::Null,
            }]
        })
        .collect();
    let plain_bpv = price_bytes(&rows, &sample.writer)? as f64 / sample.non_null as f64;
    let Some(savings) = sample.saving(plain_bpv) else {
        return Ok(None);
    };
    Ok(Some(sample.suggestion(
        DICTIONARY_UNIQUE_RULE,
        format!(
            "a dictionary on nearly-unique values at {:.2} B/value; without it the sample prices \
             at {plain_bpv:.2} B/value",
            sample.text_bpv
        ),
        "disable the dictionary for this column at write",
        savings,
    )))
}

/// The fraction of a column's sampled non-null values that are distinct.
fn unique_fraction(cells: &[Option<&[u8]>]) -> f64 {
    let values: Vec<&[u8]> = cells.iter().flatten().copied().collect();
    let distinct: HashSet<&[u8]> = values.iter().copied().collect();
    distinct.len() as f64 / values.len().max(1) as f64
}
