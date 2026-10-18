//! The backend-neutral surface model: the Python names an artifact exposes.
//!
//! Both front ends (the PyO3 bindings in `src/rust.rs`, the stub in
//! `src/python.rs`) fill the same [`Entry`] list, so a rule compares two
//! surfaces without knowing which language produced them. Model declarations
//! from the library (a record's serde fields) fill the [`ModelIndex`], which
//! names no source position — it is the second opinion a stub field is checked
//! against.

use std::collections::BTreeMap;
use std::path::PathBuf;

/// What a Python name is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// A module (a real one in the bindings, a nested-class proxy in a stub).
    Module,
    /// A function reachable from Python.
    Function,
    /// A class.
    Class,
}

impl Kind {
    /// The lower-case spelling used in output.
    pub fn as_str(self) -> &'static str {
        match self {
            Kind::Module => "module",
            Kind::Function => "function",
            Kind::Class => "class",
        }
    }
}

/// One function parameter as Python sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Param {
    /// The parameter name.
    pub name: String,
    /// Whether the parameter carries a default.
    pub has_default: bool,
}

impl Param {
    /// A parameter without a default.
    pub fn new(name: impl Into<String>, has_default: bool) -> Self {
        Self {
            name: name.into(),
            has_default,
        }
    }
}

/// One field of a record: its name and whether the model can leave it out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    /// The field name, as serde writes it.
    pub name: String,
    /// Whether the field is absent when empty (`T | None` in the stub).
    pub optional: bool,
    /// 1-based line of the name, in the stub.
    pub line: usize,
    /// 1-based byte column of the name, in the stub.
    pub column: usize,
}

impl Field {
    /// A field with its position.
    pub fn new(name: impl Into<String>, optional: bool, line: usize, column: usize) -> Self {
        Self {
            name: name.into(),
            optional,
            line,
            column,
        }
    }
}

/// One method of a class, or a property.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Method {
    /// The method name (`__init__` for a constructor).
    pub name: String,
    /// The parameters, `self`/`cls` dropped.
    pub params: Vec<Param>,
    /// 1-based line of the name.
    pub line: usize,
    /// 1-based byte column of the name.
    pub column: usize,
}

impl Method {
    /// A method with its position.
    pub fn new(name: impl Into<String>, params: Vec<Param>, line: usize, column: usize) -> Self {
        Self {
            name: name.into(),
            params,
            line,
            column,
        }
    }

    /// Whether the method is part of the public surface a stub should mirror:
    /// not a dunder, except the constructor.
    pub fn is_public(&self) -> bool {
        !self.name.starts_with("__") || self.name == "__init__"
    }
}

/// One name in the Python surface, at its source position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// The dotted path, rooted at `pqbench.native`.
    pub path: String,
    /// What the name is.
    pub kind: Kind,
    /// 1-based line of the name.
    pub line: usize,
    /// 1-based byte column of the name.
    pub column: usize,
    /// The parameters, for a function; empty otherwise.
    pub params: Vec<Param>,
    /// The canonical return shape, for a function: `Table`, `list[Table]`,
    /// `AsyncIterator[TableFile]`, `dict[str,Any]`, or `None` when unknown.
    pub returns: Option<String>,
    /// Whether the call is awaitable — a coroutine (`Some(true)`), a plain
    /// value (`Some(false)`), or not derivable (`None`).
    pub is_async: Option<bool>,
    /// The base classes, for a class; empty otherwise.
    pub bases: Vec<String>,
    /// The class-level attributes, for a class; empty otherwise.
    pub fields: Vec<Field>,
    /// The methods and properties, for a class; empty otherwise.
    pub methods: Vec<Method>,
}

impl Entry {
    /// A module entry at a position.
    pub fn module(path: impl Into<String>, line: usize, column: usize) -> Self {
        Self::new(path, Kind::Module, line, column)
    }

    /// A class entry at a position.
    pub fn class(
        path: impl Into<String>,
        line: usize,
        column: usize,
        bases: Vec<String>,
        fields: Vec<Field>,
    ) -> Self {
        Self {
            bases,
            fields,
            ..Self::new(path, Kind::Class, line, column)
        }
    }

    /// A function entry at a position.
    pub fn function(
        path: impl Into<String>,
        line: usize,
        column: usize,
        params: Vec<Param>,
        returns: Option<String>,
        is_async: Option<bool>,
    ) -> Self {
        Self {
            params,
            returns,
            is_async,
            ..Self::new(path, Kind::Function, line, column)
        }
    }

    fn new(path: impl Into<String>, kind: Kind, line: usize, column: usize) -> Self {
        Self {
            path: path.into(),
            kind,
            line,
            column,
            params: Vec::new(),
            returns: None,
            is_async: None,
            bases: Vec::new(),
            fields: Vec::new(),
            methods: Vec::new(),
        }
    }

    /// The last dotted segment: the name under its parent.
    pub fn name(&self) -> &str {
        self.path.rsplit('.').next().unwrap_or(&self.path)
    }

    /// The dotted path of the parent module, or `None` at the root.
    pub fn parent(&self) -> Option<&str> {
        self.path.rsplit_once('.').map(|(parent, _)| parent)
    }

    /// The record class a return shape names: the class inside `list[…]` or
    /// `AsyncIterator[…]`, or the shape itself, when it is one. A `dict[…]`
    /// document names none.
    pub fn returned_class(&self) -> Option<String> {
        let shape = self.returns.as_deref()?;
        if shape.starts_with("dict[") {
            return None;
        }
        let inner = shape
            .split_once('[')
            .and_then(|(_, rest)| rest.strip_suffix(']'))
            .unwrap_or(shape);
        (!inner.is_empty() && inner.chars().next().is_some_and(char::is_uppercase))
            .then(|| inner.to_owned())
    }
}

/// One side of the pair: every name the artifact exposes, from one set of files.
#[derive(Debug, Clone)]
pub struct Surface {
    /// The file the entries were read from, for a finding's location.
    pub file: PathBuf,
    /// The names, in source order.
    pub entries: Vec<Entry>,
}

impl Surface {
    /// A surface over the entries of one file.
    pub fn new(file: impl Into<PathBuf>, entries: Vec<Entry>) -> Self {
        Self {
            file: file.into(),
            entries,
        }
    }

    /// The entry at a path, if this surface exposes it.
    pub fn get(&self, path: &str) -> Option<&Entry> {
        self.entries.iter().find(|entry| entry.path == path)
    }
}

/// One field of a library model, as serde serializes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelField {
    /// The field name.
    pub name: String,
    /// Whether serde omits the field when it is empty.
    pub optional: bool,
}

/// The library's model structs: a class name to the field lists of every
/// `pub struct` of that name.
///
/// A record class names one model, but the same name can be declared twice (a
/// wire `v1::TableFile` and the streaming `table::TableFile`); the stub must
/// match one of them, so the index keeps every candidate.
#[derive(Debug, Default)]
pub struct ModelIndex {
    structs: BTreeMap<String, Vec<Vec<ModelField>>>,
}

impl ModelIndex {
    /// Add one `pub struct` and its fields.
    pub fn add(&mut self, name: impl Into<String>, fields: Vec<ModelField>) {
        let name = name.into();
        let candidates = self.structs.entry(name).or_default();
        if !candidates.contains(&fields) {
            candidates.push(fields);
        }
    }

    /// Every candidate field list for a struct name, or an empty slice.
    pub fn candidates(&self, name: &str) -> &[Vec<ModelField>] {
        self.structs.get(name).map_or(&[], Vec::as_slice)
    }

    /// Whether a name is declared at all.
    pub fn contains(&self, name: &str) -> bool {
        self.structs.contains_key(name)
    }
}
