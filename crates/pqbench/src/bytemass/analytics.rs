//! Analytics: aggregate raw per-chunk masses into a treemap hierarchy, where
//! the measure is on-disk bytes per row.

use std::collections::BTreeMap;

use serde::Serialize;

use super::raw::FileRaw;

/// One treemap node: a path segment (or the file root). Leaves have no
/// children and their `value` is the on-disk bytes/row of that column.
#[derive(Debug, Clone, Serialize)]
pub(super) struct MassNode {
    /// Label drawn on the cell: a path segment, column name, or the file root.
    #[serde(rename = "name")]
    pub label: String,
    /// Sum of the descendant leaf values (bytes/row); the treemap area.
    pub value: f64,
    /// Child nodes; empty for a leaf column.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub children: Vec<MassNode>,
}

/// Aggregate raw masses into a treemap rooted at the file.
///
/// Chunks for the same column path are summed across row groups; the leaf
/// value is that total divided by the file's row count (on-disk bytes per
/// row). The hierarchy nests by the column path's `.`-separated segments.
pub(super) fn aggregate(file: &FileRaw) -> MassNode {
    let mut totals: BTreeMap<&str, u64> = BTreeMap::new();
    for c in &file.columns {
        *totals.entry(c.column.as_str()).or_insert(0) += c.compressed_bytes;
    }
    let denom = file.num_rows.max(1) as f64;
    let mut root = branch("file");
    for (path, bytes) in totals {
        let value = bytes as f64 / denom;
        let segments: Vec<&str> = path.split('.').collect();
        insert(&mut root, &segments, value);
    }
    recompute(&mut root);
    root
}

/// A treemap node that will get a `value` (from `insert` or `recompute`).
fn branch(label: &str) -> MassNode {
    MassNode {
        label: label.into(),
        value: 0.0,
        children: Vec::new(),
    }
}

fn insert(node: &mut MassNode, segments: &[&str], value: f64) {
    if segments.is_empty() {
        node.value = value;
        return;
    }
    let seg = segments[0];
    if let Some(child) = node.children.iter_mut().find(|c| c.label == seg) {
        insert(child, &segments[1..], value);
    } else {
        let mut child = branch(seg);
        insert(&mut child, &segments[1..], value);
        node.children.push(child);
    }
}

fn recompute(node: &mut MassNode) {
    if node.children.is_empty() {
        return;
    }
    for child in &mut node.children {
        recompute(child);
    }
    node.value = node.children.iter().map(|c| c.value).sum();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bytemass::raw::RawColumn;

    #[test]
    fn aggregate_nests_by_path_and_sums_across_row_groups() {
        let file = FileRaw {
            num_rows: 100,
            columns: vec![
                RawColumn {
                    column: "text".into(),
                    compressed_bytes: 40,
                },
                RawColumn {
                    column: "text".into(),
                    compressed_bytes: 60,
                },
                RawColumn {
                    column: "a.b".into(),
                    compressed_bytes: 100,
                },
            ],
        };
        let tree = aggregate(&file);
        assert_eq!(tree.label, "file");

        let mut labels: Vec<&str> = tree.children.iter().map(|c| c.label.as_str()).collect();
        labels.sort_unstable();
        assert_eq!(labels, ["a", "text"]);

        let text = tree.children.iter().find(|c| c.label == "text").unwrap();
        assert!(text.children.is_empty());
        assert_eq!(text.value, 1.0);

        let a = tree.children.iter().find(|c| c.label == "a").unwrap();
        assert_eq!(a.children.len(), 1);
        assert_eq!(a.children[0].label, "b");
        assert_eq!(a.children[0].value, 1.0);

        assert_eq!(tree.value, 2.0);
    }
}
