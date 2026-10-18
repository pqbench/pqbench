//! The library front end: the serde models a record class mirrors.
//!
//! A record's fields come from the `pub struct` the library serializes, not
//! from the binding, so this reads the struct declarations of the library crate
//! into the [`ModelIndex`]. A field is one `pub` field; a `#[serde(skip)]`
//! field never reaches the wire and is dropped. A field is *absent-able* when
//! serde skips it at its empty value, which is what the stub writes as `| None`
//! — `skip_serializing_if = "Option::is_none"`, not the empty-collection skips.

use tree_sitter::{Node, Parser};

use crate::model::{ModelField, ModelIndex};

/// Scan one library source file and add its `pub struct` declarations.
pub fn collect(source: &str, index: &mut ModelIndex) {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_rust::LANGUAGE.into())
        .expect("the bundled Rust grammar loads");
    let Some(tree) = parser.parse(source, None) else {
        return;
    };
    let mut cursor = vec![tree.root_node()];
    while let Some(node) = cursor.pop() {
        if node.kind() == "struct_item" {
            read_struct(source, node, index);
        }
        let mut child_cursor = node.walk();
        cursor.extend(node.children(&mut child_cursor));
    }
}

/// Read one struct: its name and its public, serialized fields.
fn read_struct(source: &str, node: Node, index: &mut ModelIndex) {
    let Some(name) = node.child_by_field_name("name") else {
        return;
    };
    let Some(body) = node.child_by_field_name("body") else {
        return;
    };
    if body.kind() != "field_declaration_list" {
        return;
    }
    let mut cursor = body.walk();
    let fields = body
        .named_children(&mut cursor)
        .filter_map(|field| read_field(source, field))
        .collect();
    index.add(text(source, name), fields);
}

/// Read one field: the name and absence of a field that reaches the wire, or
/// `None` for a line that is not a public, serialized field.
fn read_field(source: &str, field: Node) -> Option<ModelField> {
    if field.kind() != "field_declaration" || !is_public(source, field) || is_skipped(source, field)
    {
        return None;
    }
    let name = field.child_by_field_name("name")?;
    Some(ModelField {
        name: text(source, name).to_owned(),
        optional: is_absent_when_empty(source, field),
    })
}

/// Whether a field carries a `pub` visibility modifier.
fn is_public(source: &str, field: Node) -> bool {
    let mut cursor = field.walk();
    let public = field
        .children(&mut cursor)
        .any(|child| child.kind() == "visibility_modifier" && text(source, child).trim() == "pub");
    public
}

/// Whether a field is attributed `#[serde(skip)]`, which never reaches the wire.
fn is_skipped(source: &str, field: Node) -> bool {
    attributes(source, field)
        .iter()
        .any(|attribute| attribute.contains("skip") && !attribute.contains("skip_serializing_if"))
}

/// Whether serde omits a field when it is empty: `skip_serializing_if` names the
/// `Option` predicate, not a collection's `is_empty`.
fn is_absent_when_empty(source: &str, field: Node) -> bool {
    attributes(source, field).iter().any(|attribute| {
        attribute.contains("skip_serializing_if") && attribute.contains("Option::is_none")
    })
}

/// A field's attributes, on itself and just before it.
fn attributes(source: &str, field: Node) -> Vec<String> {
    let mut out = Vec::new();
    let mut cursor = field.walk();
    for child in field.children(&mut cursor) {
        if child.kind() == "attribute_item" {
            out.push(text(source, child).to_owned());
        }
    }
    let mut sibling = field.prev_sibling();
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

/// The source text of a node.
fn text<'a>(source: &'a str, node: Node) -> &'a str {
    node.utf8_text(source.as_bytes()).unwrap_or("")
}
