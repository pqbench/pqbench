//! tree-sitter front end: parse a Markdown document into code blocks.
//!
//! The grammar node that matters is `fenced_code_block`, whose children include
//! the opening/closing `fenced_code_block_delimiter`, an `info_string`, and a
//! `code_fence_content`. The info string carries the language and the `run`
//! marker; the content is the block body. Each block also records the nearest
//! preceding heading (the enclosing `section`'s `atx_heading` or `setext_heading`)
//! so a generated test can be named after the section it documents.

use tree_sitter::{Node, Parser};

use crate::model::{Block, BlockInfo};

/// A reusable Markdown parser.
///
/// `Parser` is not `Sync` and re-installing the grammar per file shows up in
/// profiles; keep one per worker thread, as `aipnaming` does for Rust.
pub struct MarkdownParser {
    parser: Parser,
}

impl MarkdownParser {
    /// Build a parser with the compiled-in Markdown grammar.
    pub fn new() -> Self {
        let mut parser = Parser::new();
        parser
            .set_language(&tree_sitter_md::LANGUAGE.into())
            .expect("the bundled Markdown grammar loads");
        Self { parser }
    }

    /// Parse one document into its fenced code blocks, in source order.
    pub fn parse(&mut self, source: &str) -> Vec<Block> {
        let Some(tree) = self.parser.parse(source, None) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        Extract {
            source,
            out: &mut out,
        }
        .walk(tree.root_node());
        out
    }
}

impl Default for MarkdownParser {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse one document with a fresh parser; convenience for one-shot calls.
pub fn parse_source(source: &str) -> Vec<Block> {
    MarkdownParser::new().parse(source)
}

struct Extract<'a> {
    source: &'a str,
    out: &'a mut Vec<Block>,
}

impl Extract<'_> {
    fn walk(&mut self, node: Node) {
        self.walk_with_heading(node, None);
    }

    /// Walk a node, carrying the heading text of the enclosing section.
    ///
    /// tree-sitter nests a heading and everything under it in a `section`, so a
    /// heading applies to every block until the next heading at the same or a
    /// shallower level; the recursion passes the latest heading down.
    fn walk_with_heading(&mut self, node: Node, heading: Option<String>) {
        let mut cursor = node.walk();
        let mut children: Vec<Node> = node.children(&mut cursor).collect();

        // A heading is the first child of the `section` it opens; it names that
        // section and every block under it until a sibling heading replaces it.
        let mut current = heading;
        if node.kind() == "section" {
            if let Some(title) = children
                .first()
                .and_then(|child| heading_text(*child, self.source))
            {
                current = Some(title);
            }
        }

        for child in children.drain(..) {
            if child.kind() == "section" {
                self.walk_with_heading(child, current.clone());
            } else if child.kind() == "fenced_code_block" {
                self.record(child, current.clone());
            } else {
                self.walk_with_heading(child, current.clone());
            }
        }
    }

    fn record(&mut self, block: Node, heading: Option<String>) {
        let mut cursor = block.walk();
        let children: Vec<Node> = block.children(&mut cursor).collect();
        let opening = children
            .iter()
            .find(|child| child.kind() == "fenced_code_block_delimiter");
        let Some(opening) = opening else {
            return;
        };
        let info_node = children.iter().find(|child| child.kind() == "info_string");
        let content = children
            .iter()
            .find(|child| child.kind() == "code_fence_content");
        let info = info_node
            .map(|node| BlockInfo::parse(self.text(*node)))
            .unwrap_or_default();
        let body = content
            .map(|node| self.text(*node).to_owned())
            .unwrap_or_default();
        self.out.push(Block {
            info,
            body,
            line: opening.start_position().row + 1,
            body_line: content.map_or(opening.start_position().row + 2, |node| {
                node.start_position().row + 1
            }),
            heading,
        });
    }

    fn text(&self, node: Node) -> &str {
        node.utf8_text(self.source.as_bytes()).unwrap_or("")
    }
}

/// The text of a heading node, with its `#` marker stripped. `None` otherwise.
fn heading_text(node: Node, source: &str) -> Option<String> {
    if node.kind() != "atx_heading" && node.kind() != "setext_heading" {
        return None;
    }
    let mut cursor = node.walk();
    let text: Vec<&str> = node
        .children(&mut cursor)
        .filter(|child| child.kind() == "inline" || child.kind() == "paragraph")
        .map(|child| child.utf8_text(source.as_bytes()).unwrap_or(""))
        .collect();
    let text = text.join(" ").trim().to_owned();
    (!text.is_empty()).then_some(text)
}
