//! The PyO3 binding front end: a Rust source into the surface model.
//!
//! The binding source is read, not compiled. tree-sitter finds the structured
//! items — a `#[pyfunction]`, a `#[pyclass]` struct, the `#[pyo3(...)]`
//! attributes — and the registration calls name the Python path: `expose`
//! binds a module variable to its dotted name, `add_function`/`add_class` hang
//! a function or class under it. The wrapper a command calls (`one`, `many`,
//! `stream_listing`, `document`) is what recovers the return shape and the
//! awaitability that PyO3 erases from the signature.

use tree_sitter::{Node, Parser};

use crate::model::{Entry, Method, Param};

/// Parse one binding source file into the surface entries it exposes.
///
/// The file is a domain module (`catalog.rs`, `file.rs`); its `register`
/// function names the paths, and every entry is rooted at `pqbench.native`.
pub fn parse(source: &str) -> Vec<Entry> {
    let Some(tree) = parse_tree(source) else {
        return Vec::new();
    };
    let registry = Registry::scan(source, tree.root_node());
    let mut items = Items::default();
    walk(source, tree.root_node(), &mut items);
    surface(&registry, &items)
}

/// Parse one binding source with the bundled Rust grammar.
fn parse_tree(source: &str) -> Option<tree_sitter::Tree> {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_rust::LANGUAGE.into())
        .expect("the bundled Rust grammar loads");
    parser.parse(source, None)
}

/// What the walk finds in one file.
#[derive(Default)]
struct Items {
    functions: Vec<Function>,
    classes: Vec<Class>,
    methods: Vec<(String, Vec<Method>)>,
}

/// The entries the registration calls name: a module's Python path, a
/// `#[pyfunction]`'s name under it, a `#[pyclass]`'s.
fn surface(registry: &Registry, items: &Items) -> Vec<Entry> {
    let mut out = Vec::new();

    // A module's Python path, named by its `expose` call.
    for module in registry.modules.values() {
        out.push(Entry::module(&module.path, module.line, module.column));
    }

    // A `#[pyfunction]`'s Python name and path.
    for function in &items.functions {
        let Some(variable) = registry.function_module.get(&function.ident) else {
            continue;
        };
        let Some(module) = registry.modules.get(variable) else {
            continue;
        };
        let spec = function.spec();
        out.push(Entry::function(
            format!("{}.{}", module.path, spec.name),
            function.line,
            function.column,
            spec.params,
            spec.returns,
            spec.is_async,
        ));
    }

    // A `#[pyclass]`'s path and methods, from its registration.
    let positions: std::collections::BTreeMap<&str, (usize, usize)> = items
        .classes
        .iter()
        .map(|class| (class.ident.as_str(), (class.line, class.column)))
        .collect();
    let pymethods: std::collections::BTreeMap<&str, &Vec<Method>> = items
        .methods
        .iter()
        .map(|(owner, methods)| (owner.as_str(), methods))
        .collect();
    for (ident, receiver, line, column) in &registry.class_registrations {
        let path = if receiver == "root" {
            format!("pqbench.native.{ident}")
        } else if let Some(module) = registry.modules.get(receiver) {
            format!("{}.{ident}", module.path)
        } else {
            continue;
        };
        let (line, column) = positions
            .get(ident.as_str())
            .copied()
            .unwrap_or((*line, *column));
        let mut entry = Entry::class(path, line, column, Vec::new(), Vec::new());
        entry.methods = pymethods
            .get(ident.as_str())
            .map(|methods| (*methods).clone())
            .unwrap_or_default();
        out.push(entry);
    }

    out
}

/// One exposed module: its dotted path and the position of the name.
struct Module {
    path: String,
    line: usize,
    column: usize,
}

/// The registration calls: which module variable carries which dotted path, and
/// which function or class hangs under it.
#[derive(Default)]
struct Registry {
    /// module variable -> its dotted path, from `expose`.
    modules: std::collections::BTreeMap<String, Module>,
    /// function ident -> module variable, from `add_function`.
    function_module: std::collections::BTreeMap<String, String>,
    /// class ident -> module variable (or `root`), from `add_class`.
    class_registrations: Vec<(String, String, usize, usize)>,
}

impl Registry {
    fn scan(source: &str, root: Node) -> Self {
        let mut registry = Self::default();
        let mut cursor = vec![root];
        while let Some(node) = cursor.pop() {
            if node.kind() == "call_expression" {
                let text = text(source, node);
                registry.classify(source, node, text);
            }
            let mut child_cursor = node.walk();
            cursor.extend(node.children(&mut child_cursor));
        }
        registry
    }

    /// Read one call's text: the three registration shapes are fixed.
    fn classify(&mut self, source: &str, node: Node, call: &str) {
        if call.starts_with("crate::expose(") {
            if let (Some(dotted), Some(variable)) = (first_string(call), last_ref(call)) {
                let offset = call
                    .find('"')
                    .map_or(node.start_byte(), |index| node.start_byte() + index + 1);
                let (line, column) = position(source, offset);
                self.modules.insert(
                    variable,
                    Module {
                        path: dotted,
                        line,
                        column,
                    },
                );
            }
        }
        if let Some(index) = call.find(".add_function(") {
            let receiver = call[..index].trim().trim_start_matches('&');
            if let Some(ident) = macro_ident(&call[index..], "wrap_pyfunction!") {
                self.function_module.insert(ident, receiver.to_owned());
            }
        }
        if let Some(index) = call.find(".add_class::<") {
            let receiver = call[..index].trim().trim_start_matches('&');
            let rest = &call[index + ".add_class::<".len()..];
            if let Some(ident) = rest.split('>').next() {
                let start = node.start_position();
                self.class_registrations.push((
                    ident.trim().to_owned(),
                    receiver.to_owned(),
                    start.row + 1,
                    start.column + 1,
                ));
            }
        }
    }
}

/// The 1-based line and column of a byte offset.
fn position(source: &str, offset: usize) -> (usize, usize) {
    let offset = offset.min(source.len());
    let before = &source[..offset];
    let line = before.matches('\n').count() + 1;
    let column = before
        .rfind('\n')
        .map_or(before.len() + 1, |index| before.len() - index);
    (line, column)
}

/// One `#[pyfunction]`'s Rust identity and the pieces its Python spec needs.
struct Function {
    ident: String,
    line: usize,
    column: usize,
    /// The `#[pyo3(signature = ...)]` override, when present.
    signature: Option<Vec<Param>>,
    /// The wrapper call's return shape and awaitability.
    returns: Option<String>,
    is_async: Option<bool>,
    /// The Python name, from `#[pyo3(name = ...)]` or the identifier.
    name: String,
    /// The parameters as the Rust signature declares them.
    params: Vec<Param>,
}

impl Function {
    fn spec(&self) -> Spec {
        Spec {
            name: self.name.clone(),
            params: self
                .signature
                .clone()
                .unwrap_or_else(|| self.params.clone()),
            returns: self.returns.clone(),
            is_async: self.is_async,
        }
    }
}

/// The Python-visible signature of a binding function.
struct Spec {
    name: String,
    params: Vec<Param>,
    returns: Option<String>,
    is_async: Option<bool>,
}

/// One `#[pyclass]` struct or enum.
struct Class {
    ident: String,
    line: usize,
    column: usize,
}

/// Collect the `#[pyfunction]` functions and `#[pyclass]` types of a file.
fn walk(source: &str, root: Node, items: &mut Items) {
    let mut cursor = vec![root];
    while let Some(node) = cursor.pop() {
        collect(source, node, items);
        let mut child_cursor = node.walk();
        cursor.extend(node.children(&mut child_cursor));
    }
}

/// File one item, when it is a binding: a `#[pyfunction]`, a `#[pyclass]`, or a
/// `#[pymethods]` impl.
fn collect(source: &str, node: Node, items: &mut Items) {
    let attributes = attributes(source, node);
    match node.kind() {
        "function_item" if has(&attributes, "pyfunction") => {
            if let Some(function) = read_function(source, node, &attributes) {
                items.functions.push(function);
            }
        }
        "struct_item" | "enum_item" if has(&attributes, "pyclass") => {
            if let Some(class) = read_class(source, node, &attributes) {
                items.classes.push(class);
            }
        }
        "impl_item" if has(&attributes, "pymethods") => {
            if let Some(owner) = node.child_by_field_name("type") {
                items
                    .methods
                    .push((identifier_name(source, owner), read_methods(source, node)));
            }
        }
        _ => {}
    }
}

/// Read one `#[pyclass]` struct or enum: its Python name and position.
fn read_class(source: &str, node: Node, attributes: &[String]) -> Option<Class> {
    let name = node.child_by_field_name("name")?;
    Some(Class {
        ident: named(attributes, "name").unwrap_or_else(|| text(source, name).to_owned()),
        line: name.start_position().row + 1,
        column: name.start_position().column + 1,
    })
}

/// The methods and properties of one `#[pymethods] impl`: `#[new]` is the
/// constructor, a getter keeps its Rust name, and a `#[pyo3(name = ...)]`
/// overrides it.
fn read_methods(source: &str, node: Node) -> Vec<Method> {
    let Some(body) = node.child_by_field_name("body") else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let mut cursor = body.walk();
    for child in body.named_children(&mut cursor) {
        if child.kind() != "function_item" {
            continue;
        }
        let Some(name) = child.child_by_field_name("name") else {
            continue;
        };
        let attributes = attributes(source, child);
        let python_name = if has(&attributes, "new") {
            "__init__".to_owned()
        } else {
            named(&attributes, "name").unwrap_or_else(|| identifier_name(source, name))
        };
        let params = signature(&attributes).unwrap_or_else(|| read_params(source, child));
        out.push(Method::new(
            python_name,
            params,
            name.start_position().row + 1,
            name.start_position().column + 1,
        ));
    }
    out
}

/// Read one `#[pyfunction]`: its name, parameters, and wrapper.
fn read_function(source: &str, node: Node, attributes: &[String]) -> Option<Function> {
    let name = node.child_by_field_name("name")?;
    let ident = text(source, name).to_owned();
    let params = read_params(source, node);
    let (returns, is_async) = wrapper(source, node);
    Some(Function {
        ident,
        line: name.start_position().row + 1,
        column: name.start_position().column + 1,
        signature: signature(attributes),
        returns,
        is_async,
        name: named(attributes, "name").unwrap_or_else(|| identifier_name(source, name)),
        params,
    })
}

/// The Python-visible parameters of a Rust function: every `parameter`, minus
/// the interpreter handle (`py: Python<'_>`) and the `PyRef<Self>` receiver
/// (`slf`), neither of which reaches Python.
fn read_params(source: &str, node: Node) -> Vec<Param> {
    let Some(parameters) = node.child_by_field_name("parameters") else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let mut cursor = parameters.walk();
    for child in parameters.children(&mut cursor) {
        if child.kind() != "parameter" {
            continue;
        }
        let Some(pattern) = child.child_by_field_name("pattern") else {
            continue;
        };
        let name = identifier(text(source, pattern));
        if is_receiver(source, child, &name) {
            continue;
        }
        out.push(Param {
            name,
            has_default: false,
        });
    }
    out
}

/// Whether a parameter is a receiver rather than a Python argument: the
/// interpreter handle (`py: Python<'_>`) or a `PyRef<Self>` borrow (`slf`).
fn is_receiver(source: &str, child: Node, name: &str) -> bool {
    if name == "slf" {
        return true;
    }
    let Some(ty) = child.child_by_field_name("type") else {
        return false;
    };
    let ty = text(source, ty).trim_start();
    ty.starts_with("Python") || (ty.starts_with("PyRef") && ty.contains("Self"))
}

/// The wrapper a command calls decides the return shape and whether the call is
/// awaitable. The wrapper's last argument is the record class to attach.
fn wrapper(source: &str, node: Node) -> (Option<String>, Option<bool>) {
    let Some(body) = node.child_by_field_name("body") else {
        return (None, None);
    };
    let Some(call) = find_call(source, body, &["one", "many", "stream_listing", "document"]) else {
        return (None, None);
    };
    let function = call
        .child_by_field_name("function")
        .map(|node| text(source, node))
        .unwrap_or("");
    let model = call
        .child_by_field_name("arguments")
        .and_then(|arguments| last_argument(source, arguments))
        .map(|argument| {
            text(source, argument)
                .split("::")
                .next()
                .unwrap_or("")
                .to_owned()
        });
    match function {
        "one" => (model, Some(true)),
        "many" => (model.map(|model| format!("list[{model}]")), Some(true)),
        "stream_listing" => (
            model.map(|model| format!("AsyncIterator[{model}]")),
            Some(false),
        ),
        "document" => (Some("dict[str,Any]".to_owned()), Some(true)),
        _ => (None, None),
    }
}

/// The first call in a body whose function name is one of `names`.
fn find_call<'a>(source: &'a str, body: Node<'a>, names: &[&str]) -> Option<Node<'a>> {
    let mut cursor = vec![body];
    while let Some(node) = cursor.pop() {
        if node.kind() == "call_expression" {
            let called = node
                .child_by_field_name("function")
                .map(|function| text(source, function))
                .unwrap_or("");
            if names.contains(&called) {
                return Some(node);
            }
        }
        let mut child_cursor = node.walk();
        cursor.extend(node.children(&mut child_cursor));
    }
    None
}

/// The last argument of an argument list, ignoring the parens and commas.
fn last_argument<'a>(source: &str, arguments: Node<'a>) -> Option<Node<'a>> {
    let mut cursor = arguments.walk();
    arguments
        .children(&mut cursor)
        .filter(|child| child.is_named())
        .filter(|child| !matches!(child.kind(), "(" | ")" | ","))
        .filter(|child| !text(source, *child).trim().is_empty())
        .last()
}

/// The `#[pyo3(signature = (...))]` parameters, when the attribute is present.
fn signature(attributes: &[String]) -> Option<Vec<Param>> {
    let inner = attributes
        .iter()
        .find_map(|attribute| attribute_inner(attribute, "signature"))?;
    let list = inner.trim().strip_prefix('=')?.trim();
    let list = list
        .strip_prefix('(')
        .and_then(|rest| rest.strip_suffix(')'))
        .unwrap_or(list);
    let params = split_top(list)
        .into_iter()
        .filter(|part| !part.trim().is_empty())
        .filter(|part| !matches!(part.trim(), "/" | "*"))
        .filter_map(|part| {
            let part = part.trim();
            let (name, default) = match part.split_once('=') {
                Some((name, value)) => (name.trim(), !value.trim().is_empty()),
                None => (part, false),
            };
            let name = identifier(name);
            (!name.is_empty()).then_some(Param {
                name,
                has_default: default,
            })
        })
        .collect();
    Some(params)
}

/// The value of one key inside a `#[pyo3(...)]` attribute, e.g. `name` of
/// `#[pyo3(name = "get")]`.
fn named(attributes: &[String], key: &str) -> Option<String> {
    let inner = attributes
        .iter()
        .find_map(|attribute| attribute_inner(attribute, key))?;
    let value = inner.trim().strip_prefix('=')?.trim();
    Some(value.trim().trim_matches('"').trim().to_owned())
}

/// The text after `key` inside `#[pyo3(<key> ...)]`, when the attribute is a
/// `pyo3` attribute and names the key.
fn attribute_inner(attribute: &str, key: &str) -> Option<String> {
    let inner = attribute.strip_prefix("#[pyo3(")?.strip_suffix(")]")?;
    for part in split_top(inner) {
        let part = part.trim();
        let Some(rest) = part.strip_prefix(key) else {
            continue;
        };
        if rest.is_empty() || rest.starts_with(['=', ',', ' ', '(', ')']) {
            return Some(rest.to_owned());
        }
    }
    None
}

/// Whether any attribute is exactly `#[<name>]` (or names it with arguments).
fn has(attributes: &[String], name: &str) -> bool {
    attributes
        .iter()
        .any(|attribute| attribute.trim_start_matches("#[").starts_with(name))
}

/// The attributes on an item: its own children and its preceding siblings,
/// which is how rustfmt and the grammar split a `#[...]` from the item.
fn attributes(source: &str, item: Node) -> Vec<String> {
    let mut out = Vec::new();
    let mut child_cursor = item.walk();
    for child in item.children(&mut child_cursor) {
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

/// The source text of a node.
fn text<'a>(source: &'a str, node: Node) -> &'a str {
    node.utf8_text(source.as_bytes()).unwrap_or("")
}

/// An identifier with raw markers and mutability stripped.
fn identifier(text: &str) -> String {
    identifier_borrowed(text).to_owned()
}

fn identifier_borrowed(text: &str) -> &str {
    let text = text.trim();
    let text = text.strip_prefix("mut ").unwrap_or(text);
    text.strip_prefix("r#").unwrap_or(text)
}

/// The identifier of a name node, normalized.
fn identifier_name(source: &str, node: Node) -> String {
    identifier(text(source, node))
}

/// The first `"..."` in a call.
fn first_string(call: &str) -> Option<String> {
    let start = call.find('"')? + 1;
    let rest = &call[start..];
    let end = rest.find('"')?;
    Some(rest[..end].to_owned())
}

/// The variable of a trailing `&<ident>)` argument.
fn last_ref(call: &str) -> Option<String> {
    let tail = call.rsplit(',').next()?.trim();
    let tail = tail.trim_end_matches(')').trim();
    let tail = tail.strip_prefix('&')?;
    let name = identifier(tail);
    (!name.is_empty()).then_some(name)
}

/// The first identifier inside a `<macro>!(<ident>...)` invocation.
fn macro_ident(text: &str, macro_name: &str) -> Option<String> {
    let inner = text.split_once(macro_name)?.1.trim_start();
    let inner = inner.strip_prefix('(')?.trim_start();
    let name = identifier_borrowed(inner.split([',', ')', ' ']).next()?);
    (!name.is_empty()).then_some(name.to_owned())
}

/// Split on top-level commas, ignoring commas inside brackets, braces, parens,
/// and string literals.
pub(crate) fn split_top(text: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut in_string = false;
    let mut current = String::new();
    let mut previous = '\0';
    for character in text.chars() {
        match character {
            '"' if previous != '\\' => in_string = !in_string,
            _ => {}
        }
        if !in_string {
            match character {
                '(' | '[' | '<' | '{' => depth += 1,
                ')' | ']' | '>' | '}' => depth -= 1,
                ',' if depth <= 0 => {
                    parts.push(current.trim().to_owned());
                    current.clear();
                    previous = character;
                    continue;
                }
                _ => {}
            }
        }
        current.push(character);
        previous = character;
    }
    if !current.trim().is_empty() {
        parts.push(current.trim().to_owned());
    }
    parts
}
