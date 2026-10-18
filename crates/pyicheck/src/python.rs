//! The stub front end: a `.pyi` source into the surface model.
//!
//! A stub cannot declare a submodule of a module, so the sibling bindings write
//! a verb's module tree as nested classes; the path, not the syntax, is the
//! name. A function a module proxy attaches with `@staticmethod` is an entry;
//! any other class-level function is a method of its class, and a bare
//! annotation is a field, so the surface rule can read a module proxy's exports
//! (`ratelimit.RateLimit`) as readily as a record's.

use tree_sitter::{Node, Parser};

use crate::model::{Entry, Field, Method, Param};

/// The dotted root every stub path is hung under.
const ROOT: &str = "pqbench.native";

/// Parse one stub source into the surface entries it declares.
pub fn parse(source: &str) -> Vec<Entry> {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_python::LANGUAGE.into())
        .expect("the bundled Python grammar loads");
    let Some(tree) = parser.parse(source, None) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    walk(source, tree.root_node(), ROOT, &mut out);
    out
}

/// Walk one block, filling entries, with the dotted prefix so far.
fn walk(source: &str, node: Node, prefix: &str, out: &mut Vec<Entry>) {
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        let Some(definition) = Definition::resolve(child) else {
            continue;
        };
        match definition.node.kind() {
            "class_definition" => read_class(source, definition.node, prefix, out),
            "function_definition"
                if prefix == ROOT || is_staticmethod(source, definition.decorators) =>
            {
                push_function(source, definition.node, prefix, out);
            }
            _ => {}
        }
    }
}

/// A block child that is a definition: the `function_definition` or
/// `class_definition` itself, and the node carrying its decorators (the
/// definition itself when it is undecorated).
struct Definition<'a> {
    node: Node<'a>,
    decorators: Node<'a>,
}

impl<'a> Definition<'a> {
    /// Resolve a block child to a definition, or `None` for anything else.
    fn resolve(child: Node<'a>) -> Option<Self> {
        match child.kind() {
            "class_definition" | "function_definition" => Some(Self {
                node: child,
                decorators: child,
            }),
            "decorated_definition" => Some(Self {
                node: child.child_by_field_name("definition")?,
                decorators: child,
            }),
            _ => None,
        }
    }
}

/// Read one class: its entry, its fields, its methods, and the module proxies
/// and static verbs nested under it.
fn read_class(source: &str, node: Node, prefix: &str, out: &mut Vec<Entry>) {
    let Some(name) = node.child_by_field_name("name") else {
        return;
    };
    let path = format!("{prefix}.{}", text(source, name));
    let bases = bases(source, node);
    let fields = fields(source, node);
    let methods = methods(source, node);
    out.push(Entry::class(
        &path,
        name.start_position().row + 1,
        name.start_position().column + 1,
        bases,
        fields,
    ));
    if let Some(entry) = out.last_mut() {
        entry.methods = methods;
    }
    // Recurse into the body for nested module proxies and static verbs.
    if let Some(body) = node.child_by_field_name("body") {
        walk(source, body, &path, out);
    }
}

/// A class's base classes, in source order.
fn bases(source: &str, node: Node) -> Vec<String> {
    let Some(superclasses) = node.child_by_field_name("superclasses") else {
        return Vec::new();
    };
    let mut cursor = superclasses.walk();
    superclasses
        .named_children(&mut cursor)
        .map(|base| text(source, base).to_owned())
        .collect()
}

/// A class's bare annotations: the identifier and type of a class-level
/// statement (`name: str`, `uid: str | None`).
fn fields(source: &str, node: Node) -> Vec<Field> {
    let Some(body) = node.child_by_field_name("body") else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let mut cursor = body.walk();
    for child in body.named_children(&mut cursor) {
        if child.kind() != "expression_statement" {
            continue;
        }
        let statement = text(source, child);
        let Some((name, annotation)) = statement.split_once(':') else {
            continue;
        };
        let name = name.trim();
        if !is_identifier(name) {
            continue;
        }
        let start = child.start_position();
        out.push(Field::new(
            name,
            is_optional(annotation),
            start.row + 1,
            start.column + 1,
        ));
    }
    out
}

/// Whether a name is a plain identifier.
fn is_identifier(name: &str) -> bool {
    let mut characters = name.chars();
    let Some(first) = characters.next() else {
        return false;
    };
    (first.is_ascii_alphabetic() || first == '_')
        && name
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '_')
}

/// Whether an annotation can be `None`: a `| None` at the top level, not a
/// `None` inside a collection's brackets (`dict[str, str | None]` is present).
fn is_optional(annotation: &str) -> bool {
    let mut depth = 0i32;
    let mut top = String::new();
    for character in annotation.chars() {
        match character {
            '[' | '(' | '{' => depth += 1,
            ']' | ')' | '}' => depth -= 1,
            _ if depth == 0 => top.push(character),
            _ => {}
        }
    }
    top.contains("None")
}

/// A class's methods and properties: every class-level function that is not a
/// static verb.
fn methods(source: &str, node: Node) -> Vec<Method> {
    let Some(body) = node.child_by_field_name("body") else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let mut cursor = body.walk();
    for child in body.named_children(&mut cursor) {
        let Some(definition) = Definition::resolve(child) else {
            continue;
        };
        if definition.node.kind() != "function_definition"
            || is_staticmethod(source, definition.decorators)
        {
            continue;
        }
        let Some(name) = definition.node.child_by_field_name("name") else {
            continue;
        };
        out.push(Method::new(
            text(source, name),
            params(source, definition.node),
            name.start_position().row + 1,
            name.start_position().column + 1,
        ));
    }
    out
}

/// Whether a definition (or its decorator wrapper) carries `@staticmethod`.
fn is_staticmethod(source: &str, node: Node) -> bool {
    let mut cursor = node.walk();
    let static_method = node
        .children(&mut cursor)
        .any(|child| child.kind() == "decorator" && text(source, child).contains("staticmethod"));
    static_method
}

/// Emit one function entry: a module-level or static verb.
fn push_function(source: &str, node: Node, prefix: &str, out: &mut Vec<Entry>) {
    let Some(name) = node.child_by_field_name("name") else {
        return;
    };
    out.push(Entry::function(
        format!("{prefix}.{}", text(source, name)),
        name.start_position().row + 1,
        name.start_position().column + 1,
        params(source, node),
        returns(source, node),
        Some(is_awaitable(source, node)),
    ));
}

/// The parameters as Python sees them: `self`/`cls` and separators dropped.
fn params(source: &str, node: Node) -> Vec<Param> {
    let Some(parameters) = node.child_by_field_name("parameters") else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let mut cursor = parameters.walk();
    for child in parameters.named_children(&mut cursor) {
        let (name, has_default) = match child.kind() {
            "identifier" => (text(source, child).to_owned(), false),
            "typed_parameter" => (parameter_name(source, child), false),
            "default_parameter" => (parameter_name(source, child), true),
            "typed_default_parameter" => (parameter_name(source, child), true),
            _ => continue,
        };
        if name.is_empty() || name == "self" || name == "cls" {
            continue;
        }
        out.push(Param::new(name, has_default));
    }
    out
}

/// A parameter's name: the `name` field, or the first identifier in the node.
fn parameter_name(source: &str, node: Node) -> String {
    if let Some(name) = node.child_by_field_name("name") {
        return text(source, name).to_owned();
    }
    let mut cursor = node.walk();
    let name = node
        .named_children(&mut cursor)
        .find(|child| child.kind() == "identifier")
        .map(|child| text(source, child).to_owned())
        .unwrap_or_default();
    name
}

/// The declared return annotation, with the `->` removed.
fn returns(source: &str, node: Node) -> Option<String> {
    let return_type = node.child_by_field_name("return_type")?;
    let text = text(source, return_type).trim();
    Some(text.trim_start_matches("->").trim().to_owned())
}

/// Whether the definition is `async`.
fn is_awaitable(source: &str, node: Node) -> bool {
    text(source, node).trim_start().starts_with("async")
}

/// The source text of a node.
fn text<'a>(source: &'a str, node: Node) -> &'a str {
    node.utf8_text(source.as_bytes()).unwrap_or("")
}
