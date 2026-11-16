//! `dictionary-unique`: a dictionary page on nearly-unique values priced
//! without it.

use std::collections::HashSet;

use crate::third_party::parquet::api::Value;

use super::price::price_bytes;
use super::writer::Writer;
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
    let plain_bpv = price_bytes(&rows, &sample.rewrite)? as f64 / sample.non_null as f64;
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
        remedy(sample.writer_kind(), sample.leaf),
        savings,
    )))
}

/// The setting that disables one column's dictionary, in the writer's terms.
fn remedy(writer: Writer, leaf: &str) -> String {
    match writer {
        Writer::ParquetMr => format!(
            "disable the dictionary for this column: spark.hadoop.parquet.enable.dictionary#{leaf}=false"
        ),
        Writer::ArrowCpp => format!(
            "disable the dictionary for this column: leave {leaf} out of pyarrow's use_dictionary"
        ),
        Writer::ArrowRs => {
            format!("disable the dictionary for this column: set_column_dictionary_enabled({leaf:?}, false)")
        }
        Writer::Photon => {
            "write this column with a writer that can disable its dictionary: Photon has no \
             per-column setting"
                .to_string()
        }
        Writer::Other => "disable the dictionary for this column at write".to_string(),
    }
}

/// The fraction of a column's sampled non-null values that are distinct.
fn unique_fraction(cells: &[Option<&[u8]>]) -> f64 {
    let values: Vec<&[u8]> = cells.iter().flatten().copied().collect();
    let distinct: HashSet<&[u8]> = values.iter().copied().collect();
    distinct.len() as f64 / values.len().max(1) as f64
}

#[cfg(test)]
mod tests {
    use super::remedy;
    use crate::lint::writer::Writer;

    #[test]
    fn each_writer_gets_a_setting_it_has() {
        assert_eq!(
            remedy(Writer::ParquetMr, "id"),
            "disable the dictionary for this column: spark.hadoop.parquet.enable.dictionary#id=false"
        );
        assert_eq!(
            remedy(Writer::ArrowRs, "id"),
            "disable the dictionary for this column: set_column_dictionary_enabled(\"id\", false)"
        );
        assert!(remedy(Writer::ArrowCpp, "id").contains("use_dictionary"));
        assert!(remedy(Writer::Photon, "id").contains("no per-column setting"));
        assert!(remedy(Writer::Other, "id").contains("at write"));
    }
}
