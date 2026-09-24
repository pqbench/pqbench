//! Presentation: render bytemass stats as CLI text.
//!
//! This is the agent-facing output: a terminal table of the per-column on-disk
//! bytes per row (the measure), sorted descending, plus the total. The
//! composable data and the browser picture come from [`super::json`] and
//! [`super::d3`].

use crate::parquet_helpers::Error;

use super::aggregate::tree;
use super::analytics::MassNode;
use super::api::MassRow;

/// Render the per-column byte-mass stats as a text table.
///
/// # Errors
/// Returns [`Error`] if a byte total overflows while aggregating.
pub fn render_text(rows: &[MassRow]) -> Result<String, Error> {
    Ok(render(&tree(rows)?))
}

/// Render a byte-mass tree as a text table.
fn render(tree: &MassNode) -> String {
    let mut out = String::new();
    out.push_str(&format!("bytemass: {}\n", tree.label));
    out.push_str(&format!("{:<32} {:>12}\n", "column", "bytes/row"));
    let mut columns = Vec::new();
    for child in &tree.children {
        leaves(child, "", &mut columns);
    }
    columns.sort_by(|a, b| b.1.total_cmp(&a.1));
    for (path, value) in &columns {
        out.push_str(&format!("{path:<32} {value:>12.2}\n"));
    }
    out.push_str(&"-".repeat(44));
    out.push('\n');
    out.push_str(&format!("{:<32} {:>12.2}\n", "total", tree.value));
    out
}

/// Collect leaf columns as (dotted path, bytes per row) in preorder.
fn leaves(node: &MassNode, prefix: &str, out: &mut Vec<(String, f64)>) {
    let path = if prefix.is_empty() {
        node.label.clone()
    } else {
        format!("{prefix}.{}", node.label)
    };
    if node.children.is_empty() {
        out.push((path, node.value));
    } else {
        for child in &node.children {
            leaves(child, &path, out);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bytemass::api::MassRow;

    fn rows(columns: &[(&str, u64)]) -> Vec<MassRow> {
        columns
            .iter()
            .map(|(column, bytes)| MassRow {
                uri: "sample.parquet".into(),
                size_bytes: 0,
                row_count: 1,
                column: (*column).into(),
                compressed_bytes: *bytes,
                uncompressed_bytes: *bytes,
                codec: "SNAPPY".into(),
            })
            .collect()
    }

    #[test]
    fn render_lists_columns_sorted_by_bytes_per_row() {
        let text = render_text(&rows(&[("text", 2), ("a.b", 1)])).unwrap();
        assert!(text.contains("bytemass: sample.parquet"));
        assert!(text.contains("text"));
        assert!(text.contains("a.b"));
        assert!(text.contains("total"));
        // text (2.00) sorts above a.b (1.00)
        let text_pos = text.find("text").unwrap();
        let ab_pos = text.find("a.b").unwrap();
        assert!(text_pos < ab_pos);
    }
}
