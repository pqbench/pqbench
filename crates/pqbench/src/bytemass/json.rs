//! Presentation: serialize the byte-mass tree as `{name, value, children}` JSON.
//!
//! This is the composable output of `bytemass`. The `d3` page builder consumes
//! it, and a CLI `--json` flag lets any other tool consume it.

use serde_json;

use super::aggregate::tree;
use super::api::MassRow;
use crate::parquet_helpers::Error;

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error(format!("json: {e}"))
    }
}

/// Serialize the byte-mass tree as `{name, value, children}` JSON (pretty).
///
/// # Errors
/// Returns [`Error`] if aggregation overflows or serialization fails.
pub fn render_json(rows: &[MassRow]) -> Result<String, Error> {
    Ok(serde_json::to_string_pretty(&tree(rows)?)?)
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
                num_rows: 1,
                column: (*column).into(),
                compressed_bytes: *bytes,
                uncompressed_bytes: *bytes,
                codec: "SNAPPY".into(),
            })
            .collect()
    }

    #[test]
    fn render_json_has_d3_shape() {
        let json = render_json(&rows(&[("a.b", 2)])).unwrap();
        assert!(json.contains("\"name\": \"f\""));
        assert!(json.contains("\"children\": ["));
        assert!(json.contains("\"name\": \"b\""));
        assert!(json.contains("\"value\": 2.0"));
        // serde emits no trailing commas; leaves omit "children".
        assert!(!json.contains("\"children\": []"));
    }
}
