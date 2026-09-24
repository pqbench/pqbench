//! tree-sitter front end: parse Rust source into the declaration model.

use tree_sitter::{Node, Parser};

use crate::decl::{DeclKind, Declaration};

/// A reusable Rust parser.
///
/// `Parser` is not `Sync`, and installing the grammar on every file shows up
/// in profiles; keep one per worker thread.
pub struct RustParser {
    parser: Parser,
}

impl RustParser {
    /// Build a parser with the compiled-in Rust grammar.
    pub fn new() -> Self {
        let mut parser = Parser::new();
        parser
            .set_language(&tree_sitter_rust::LANGUAGE.into())
            .expect("the bundled Rust grammar loads");
        Self { parser }
    }

    /// Parse one source text into declarations, in source order.
    pub fn parse(&mut self, source: &str) -> Vec<Declaration> {
        let Some(tree) = self.parser.parse(source, None) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        Extract {
            source,
            out: &mut out,
        }
        .walk(tree.root_node(), None);
        out
    }
}

impl Default for RustParser {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse one source text with a fresh parser; convenience for one-shot calls.
pub fn parse_source(source: &str) -> Vec<Declaration> {
    RustParser::new().parse(source)
}

struct Extract<'a> {
    source: &'a str,
    out: &'a mut Vec<Declaration>,
}

impl Extract<'_> {
    fn walk(&mut self, node: Node, owner: Option<&str>) {
        match node.kind() {
            "struct_item" | "union_item" | "enum_item" | "trait_item" => {
                let Some(name) = node.child_by_field_name("name") else {
                    return;
                };
                let kind = match node.kind() {
                    "struct_item" => DeclKind::Struct,
                    "union_item" => DeclKind::Union,
                    "enum_item" => DeclKind::Enum,
                    _ => DeclKind::Trait,
                };
                let name_text = self.text(name).to_owned();
                self.record(name, kind, owner, None, node);
                self.walk_children(node, Some(&name_text));
                return;
            }
            "impl_item" => {
                let impl_owner = node
                    .child_by_field_name("type")
                    .map(|ty| self.text(ty).to_owned());
                self.walk_children(node, impl_owner.as_deref().or(owner));
                return;
            }
            "mod_item" => {
                if let Some(name) = node.child_by_field_name("name") {
                    self.record(name, DeclKind::Module, owner, None, node);
                }
                self.walk_children(node, None);
                return;
            }
            _ => {}
        }

        match node.kind() {
            "type_item" | "associated_type" => {
                if let Some(name) = node.child_by_field_name("name") {
                    let ty = node
                        .child_by_field_name("type")
                        .map(|t| self.text(t).to_owned());
                    self.record(name, DeclKind::Alias, owner, ty, node);
                }
            }
            "function_item" | "function_signature_item" => {
                if let Some(name) = node.child_by_field_name("name") {
                    let receiver = self.has_receiver(node);
                    let kind = match (owner, receiver) {
                        (_, true) => DeclKind::Method,
                        (Some(_), false) => DeclKind::AssociatedFunction,
                        (None, false) => DeclKind::Function,
                    };
                    let ty = node
                        .child_by_field_name("return_type")
                        .map(|t| self.text(t).to_owned());
                    self.record(name, kind, owner, ty, node);
                }
                self.walk_children(node, None);
                return;
            }
            "field_declaration" => {
                if let Some(name) = node.child_by_field_name("name") {
                    let ty = node
                        .child_by_field_name("type")
                        .map(|t| self.text(t).to_owned());
                    self.record(name, DeclKind::Field, owner, ty, node);
                }
            }
            "enum_variant" => {
                if let Some(name) = node.child_by_field_name("name") {
                    self.record(name, DeclKind::Variant, owner, None, node);
                }
            }
            "const_item" => {
                if let Some(name) = node.child_by_field_name("name") {
                    let ty = node
                        .child_by_field_name("type")
                        .map(|t| self.text(t).to_owned());
                    self.record(name, DeclKind::Constant, owner, ty, node);
                }
            }
            "static_item" => {
                if let Some(name) = node.child_by_field_name("name") {
                    let ty = node
                        .child_by_field_name("type")
                        .map(|t| self.text(t).to_owned());
                    self.record(name, DeclKind::Static, owner, ty, node);
                }
            }
            "macro_definition" => {
                if let Some(name) = node.child_by_field_name("name") {
                    self.record(name, DeclKind::Macro, owner, None, node);
                }
            }
            _ => {}
        }
        self.walk_children(node, owner);
    }

    fn has_receiver(&self, node: Node) -> bool {
        let Some(params) = node.child_by_field_name("parameters") else {
            return false;
        };
        let mut cursor = params.walk();
        let receiver = params
            .children(&mut cursor)
            .any(|child| child.kind() == "self_parameter");
        receiver
    }

    fn walk_children(&mut self, node: Node, owner: Option<&str>) {
        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            self.walk(child, owner);
        }
    }

    fn record(
        &mut self,
        name: Node,
        kind: DeclKind,
        owner: Option<&str>,
        type_name: Option<String>,
        item: Node,
    ) {
        let start = name.start_position();
        let docs = self.has_docs(item);
        let raw_name = self.text(name);
        let name_text = raw_name.strip_prefix("r#").unwrap_or(raw_name);
        self.out.push(Declaration {
            name: name_text.to_owned(),
            kind,
            owner: owner.map(str::to_owned),
            type_name,
            line: start.row + 1,
            column: start.column + 1,
            name_range: name.start_byte()..name.end_byte(),
            docs,
        });
    }

    fn text(&self, node: Node) -> &str {
        node.utf8_text(self.source.as_bytes()).unwrap_or("")
    }

    /// Whether a doc comment is attached to the item.
    ///
    /// tree-sitter attaches a leading comment either to the item itself (then
    /// the item's start byte points at the comment) or as the item's previous
    /// sibling; both shapes are checked, and attribute lines in between are
    /// skipped.
    fn has_docs(&self, item: Node) -> bool {
        if is_doc_text(self.source[item.start_byte()..].trim_start()) {
            return true;
        }
        let mut sibling = item.prev_sibling();
        while let Some(node) = sibling {
            match node.kind() {
                "line_comment" | "block_comment" => {
                    return is_doc_text(self.text(node).trim_start());
                }
                "attribute_item" => sibling = node.prev_sibling(),
                _ => break,
            }
        }
        let mut lines = self.source[..item.start_byte()].split('\n').rev();
        let _same_line = lines.next();
        for line in lines.take(64) {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            if is_doc_text(line) || line.ends_with("*/") {
                return true;
            }
            if line.starts_with("#[") {
                continue;
            }
            return false;
        }
        false
    }
}

/// Whether a comment line is a doc comment.
fn is_doc_text(text: &str) -> bool {
    text.starts_with("///")
        || text.starts_with("//!")
        || text.starts_with("/**")
        || text.starts_with("/*!")
}
