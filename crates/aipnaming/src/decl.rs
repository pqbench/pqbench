//! The backend-neutral declaration model every naming rule consumes.

use std::ops::Range;

/// The syntactic role of a named declaration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeclKind {
    /// A `struct` item.
    Struct,
    /// An `enum` item.
    Enum,
    /// A `union` item.
    Union,
    /// A `trait` item.
    Trait,
    /// A `type` alias, including associated types.
    Alias,
    /// A free function.
    Function,
    /// A function with a `self` receiver.
    Method,
    /// A function under an `impl` or trait without a `self` receiver.
    AssociatedFunction,
    /// A named struct or struct-variant field.
    Field,
    /// An enum variant.
    Variant,
    /// A `const` item.
    Constant,
    /// A `static` item.
    Static,
    /// A `mod` item.
    Module,
    /// A `macro_rules!` definition.
    Macro,
}

/// One named declaration found in the source, in source order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Declaration {
    /// The identifier as written.
    pub name: String,
    /// The syntactic role.
    pub kind: DeclKind,
    /// The enclosing type (for fields and variants) or impl/trait (for
    /// functions); `None` for top-level items.
    pub owner: Option<String>,
    /// The type as written, for fields and items that carry one.
    pub type_name: Option<String>,
    /// 1-based line of the first name byte.
    pub line: usize,
    /// 1-based byte column of the first name byte.
    pub column: usize,
    /// Byte range of the identifier in the source.
    pub name_range: Range<usize>,
    /// Whether a doc comment (`///`, `//!`, `/**`, or `#[doc]`) precedes it.
    pub docs: bool,
}

impl Declaration {
    /// Whether this declaration names a type.
    pub fn is_type(&self) -> bool {
        matches!(
            self.kind,
            DeclKind::Struct | DeclKind::Enum | DeclKind::Union | DeclKind::Trait | DeclKind::Alias
        )
    }

    /// Whether a field or item is written as `bool`.
    ///
    /// Aliases and paths (`core::primitive::bool`) are not resolved; this is
    /// syntax, not type checking.
    pub fn is_bool(&self) -> bool {
        self.type_name.as_deref() == Some("bool")
    }

    /// The outer named type constructor, e.g. `Vec` for `Vec<u8>`, `Option`
    /// for `Option<Report>`, or `HashMap` for `HashMap<K, V>`. References,
    /// slices, arrays, tuples, raw pointers, and `impl`/`dyn` bounds have no
    /// single named constructor and return `None`.
    pub fn type_constructor(&self) -> Option<&str> {
        let mut ty = self.type_name.as_deref()?.trim();
        ty = ty.trim_start_matches('&').trim_start();
        ty = ty.strip_prefix("mut ").unwrap_or(ty).trim_start();
        if ty.starts_with(['[', '(', '*', '!']) {
            return None;
        }
        let end = ty
            .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == ':'))
            .unwrap_or(ty.len());
        let path = ty[..end].trim_end_matches(':');
        if path.is_empty() {
            None
        } else {
            Some(path)
        }
    }

    /// The last path segment of [`Self::type_constructor`], e.g. `Vec` for
    /// `std::vec::Vec`.
    pub fn type_constructor_name(&self) -> Option<&str> {
        self.type_constructor()
            .map(|path| path.rsplit("::").next().unwrap_or(path))
    }
}
