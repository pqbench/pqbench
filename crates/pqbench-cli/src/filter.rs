//! Include/exclude names with a glob or an exact prefix.

/// Keep names that match `--include` and do not match `--exclude`.
///
/// A pattern with `*`, `?`, or `[` is a glob. Anything else is an exact name
/// or a prefix (`catalog` matches `catalog.schema.table` and `sales` matches
/// `sales/events`).
#[derive(Clone, Default)]
pub(crate) struct NameFilter {
    include: Vec<String>,
    exclude: Vec<String>,
}

impl NameFilter {
    pub(crate) fn new(include: Vec<String>, exclude: Vec<String>) -> Self {
        Self { include, exclude }
    }

    pub(crate) fn keeps(&self, name: &str) -> bool {
        let kept = self.include.is_empty()
            || self
                .include
                .iter()
                .any(|pattern| matches_name(name, pattern));
        kept && !self
            .exclude
            .iter()
            .any(|pattern| matches_name(name, pattern))
    }

    /// Catalogs `--include` can name without walking `/catalogs`.
    pub(crate) fn catalog_scope(&self) -> Option<Vec<String>> {
        literal_heads(&self.include, 0)
    }

    /// Schemas `--include` can name inside `catalog` without walking `/schemas`.
    pub(crate) fn schema_scope(&self, catalog: &str) -> Option<Vec<String>> {
        let patterns: Vec<String> = self
            .include
            .iter()
            .filter(|pattern| pattern_applies_to_catalog(pattern, catalog))
            .cloned()
            .collect();
        if patterns.is_empty() {
            return None;
        }
        literal_heads(&patterns, 1)
    }
}

fn matches_name(name: &str, pattern: &str) -> bool {
    if is_glob(pattern) {
        return glob::Pattern::new(pattern)
            .map(|glob| glob.matches(name))
            .unwrap_or(false);
    }
    name == pattern
        || name.starts_with(&format!("{pattern}."))
        || name.starts_with(&format!("{pattern}/"))
}

fn is_glob(pattern: &str) -> bool {
    pattern.contains('*') || pattern.contains('?') || pattern.contains('[')
}

fn split_name(name: &str) -> Vec<&str> {
    name.split(['.', '/'])
        .filter(|part| !part.is_empty())
        .collect()
}

fn literal_heads(patterns: &[String], index: usize) -> Option<Vec<String>> {
    if patterns.is_empty() {
        return None;
    }
    let mut heads = Vec::new();
    for pattern in patterns {
        let parts = split_name(pattern);
        let Some(part) = parts.get(index) else {
            continue;
        };
        if is_glob(part) {
            return None;
        }
        if !heads.iter().any(|have| have == part) {
            heads.push((*part).to_string());
        }
    }
    if heads.is_empty() {
        None
    } else {
        Some(heads)
    }
}

fn pattern_applies_to_catalog(pattern: &str, catalog: &str) -> bool {
    let Some(first) = split_name(pattern).into_iter().next() else {
        return false;
    };
    if is_glob(first) {
        return glob::Pattern::new(first)
            .map(|glob| glob.matches(catalog))
            .unwrap_or(false);
    }
    first == catalog
}
