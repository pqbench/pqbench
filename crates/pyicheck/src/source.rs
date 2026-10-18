//! Reading a tree-sitter node's source: the two readers the front ends share.
//!
//! `rust`, `python`, and `library` each parse a different grammar, but they read
//! a node's text and an item's attributes the same way, so the readers live
//! here rather than in each.

use tree_sitter::Node;

/// The source text of a node.
pub(crate) fn text<'a>(source: &'a str, node: Node) -> &'a str {
    node.utf8_text(source.as_bytes()).unwrap_or("")
}

/// An item's attributes: its own `#[...]` children and the ones just before it,
/// which is how rustfmt and the grammar split an attribute from its item.
pub(crate) fn attributes(source: &str, item: Node) -> Vec<String> {
    let mut out = Vec::new();
    let mut cursor = item.walk();
    for child in item.children(&mut cursor) {
        if child.kind() == "attribute_item" {
            out.push(text(source, child).to_owned());
        }
    }
    let mut sibling = item.prev_sibling();
    while let Some(node) = sibling {
        match node.kind() {
            "attribute_item" => {
                out.push(text(source, node).to_owned());
                sibling = node.prev_sibling();
            }
            "line_comment" | "block_comment" => sibling = node.prev_sibling(),
            _ => break,
        }
    }
    out
}
