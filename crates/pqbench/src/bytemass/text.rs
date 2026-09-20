//! Presentation: render bytemass stats as CLI text.
//!
//! This is the agent-facing output: a terminal table of the per-column on-disk
//! bytes per row (the measure), sorted descending, plus the total. The
//! composable data and the browser picture come from [`super::json`] and
//! [`super::d3`].

use super::analytics::MassNode;

/// Render the per-column byte-mass stats as a text table.
pub(super) fn render(tree: &MassNode) -> String {
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
    use crate::bytemass::analytics::MassNode;

    fn leaf(label: &str, value: f64) -> MassNode {
        MassNode {
            label: label.into(),
            value,
            children: vec![],
        }
    }

    #[test]
    fn render_lists_columns_sorted_by_bytes_per_row() {
        let tree = MassNode {
            label: "sample.parquet".into(),
            value: 3.0,
            children: vec![
                leaf("text", 2.0),
                MassNode {
                    label: "a".into(),
                    value: 1.0,
                    children: vec![leaf("b", 1.0)],
                },
            ],
        };
        let text = render(&tree);
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
