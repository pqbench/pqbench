//! `dictionary-unique`: a dictionary page on nearly-unique values, priced
//! against the same writer with the dictionary off.

use std::collections::HashSet;

use super::control::{self, Measurement};
use super::writer::Writer;
use super::{ColumnSample, Error, Finding};

/// The rule id a suggestion carries.
pub const DICTIONARY_UNIQUE_RULE: &str = "dictionary-unique";

/// Values are nearly unique when at least this fraction of the sampled
/// non-null values are distinct.
const UNIQUE_FRACTION: f64 = 0.5;

/// The finding this column earns when its chunk carries a dictionary page and
/// the values are nearly unique.
pub(super) fn detect(sample: &ColumnSample<'_>) -> Option<Finding> {
    if !sample.mass.dictionary || unique_fraction(&sample.cells) < UNIQUE_FRACTION {
        return None;
    }
    Some(Finding {
        message: format!(
            "a dictionary on nearly-unique values at {:.2} B/value",
            sample.text_bpv
        ),
        remedy: remedy(sample.writer_kind(), sample.leaf),
        measure,
    })
}

/// Price the dictionary arm against the same values with the dictionary off,
/// both written by the same writer at the file's codec and level.
fn measure(sample: &ColumnSample<'_>) -> Result<Option<Measurement>, Error> {
    let rows = sample.text_rows();
    control::compare(sample, &rows, &rows, false, None)
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
        Writer::Photon => format!(
            "disable the dictionary for this column: \
             spark.hadoop.parquet.enable.dictionary#{leaf}=false (Photon forwarding unverified)"
        ),
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
        assert!(remedy(Writer::Photon, "id").contains("parquet.enable.dictionary#id=false"));
        assert!(remedy(Writer::Photon, "id").contains("unverified"));
        assert!(remedy(Writer::Other, "id").contains("at write"));
    }
}
