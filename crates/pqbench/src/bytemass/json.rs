//! Data layer: serialize the byte-mass tree as `{name, value, children}` JSON.
//!
//! This is the composable output of `bytemass`. The `d3` page builder consumes
//! it, and a CLI `--json` flag lets any other tool consume it.

use serde_json;

use super::analytics::MassNode;
use crate::parquet_helpers::Error;

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error(format!("json: {e}"))
    }
}

/// Serialize a treemap node as `{name, value, children}` JSON (pretty).
///
/// # Errors
/// Returns [`Error`] only if serialization fails; for a [`MassNode`] this is
/// impossible, since its fields are always serializable.
pub(super) fn tree(node: &MassNode) -> Result<String, Error> {
    Ok(serde_json::to_string_pretty(node)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bytemass::analytics::MassNode;

    fn leaf(label: &str, value: f64) -> MassNode {
        MassNode {
            label: label.into(),
            value,
            children: vec![],
        }
    }

    #[test]
    fn tree_is_valid_json_with_d3_shape() {
        let root = MassNode {
            label: "f".into(),
            value: 2.0,
            children: vec![MassNode {
                label: "a".into(),
                value: 2.0,
                children: vec![leaf("b", 2.0)],
            }],
        };
        let json = tree(&root).unwrap();
        assert!(json.contains("\"name\": \"f\""));
        assert!(json.contains("\"children\": ["));
        assert!(json.contains("\"name\": \"b\""));
        assert!(json.contains("\"value\": 2.0"));
        // serde emits no trailing commas; leaves omit "children".
        assert!(!json.contains("\"children\": []"));
    }
}
