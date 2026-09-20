//! Presentation: serialize the byte-mass table as flat, composable JSON.
//!
//! This is the composable output of `bytemass`: one record per column carrying
//! its on-disk byte totals and codecs. The `d3` page builder folds the same rows
//! into its own treemap hierarchy, so the JSON carries no treemap shape.

use serde_json;

use super::aggregate::aggregate;
use super::api::MassRow;
use crate::parquet_helpers::Error;

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error(format!("json: {e}"))
    }
}

/// Serialize the per-column byte masses as flat JSON (pretty).
///
/// # Errors
/// Returns [`Error`] if aggregation overflows or serialization fails.
pub fn render_json(rows: &[MassRow]) -> Result<String, Error> {
    Ok(serde_json::to_string_pretty(&aggregate(rows)?)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bytemass::api::MassRow;

    fn rows(columns: &[(&str, u64)]) -> Vec<MassRow> {
        columns
            .iter()
            .map(|(column, bytes)| MassRow {
                file: "f".into(),
                size: 0,
                num_rows: 10,
                column: (*column).into(),
                compressed_bytes: *bytes,
                uncompressed_bytes: *bytes,
                codec: "SNAPPY".into(),
            })
            .collect()
    }

    #[test]
    fn render_json_is_flat_per_column_records() {
        let json = render_json(&rows(&[("a.b", 20), ("text", 10)])).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["file_count"], 1);
        assert_eq!(value["num_rows"], 10);
        let columns = value["columns"].as_array().unwrap();
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0]["path"], "a.b");
        assert_eq!(columns[0]["compressed_bytes"], 20);
        assert_eq!(columns[0]["codecs"], serde_json::json!(["SNAPPY"]));
        // The JSON is a flat table, not a d3 treemap hierarchy.
        assert!(value.get("name").is_none());
        assert!(value.get("children").is_none());
    }
}
