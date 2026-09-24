//! Include/exclude Unity FQNs: catalog, catalog.schema, catalog.schema.table.
//!
//! A pattern with `*`, `?`, or `[` is a glob, matched against the whole FQN
//! or component-wise (`main.*.events`). Anything else is an exact FQN or a
//! prefix (`main` keeps `main.default.events`).

/// Keep names that match `--include` and do not match `--exclude`.
#[derive(Clone, Default)]
pub struct NameFilter {
    include: Vec<String>,
    exclude: Vec<String>,
}

impl NameFilter {
    pub fn new(include: Vec<String>, exclude: Vec<String>) -> Self {
        Self { include, exclude }
    }

    /// A complete table FQN, or a directory lake name.
    pub fn keeps(&self, name: &str) -> bool {
        self.included(name) && !self.excluded(name)
    }

    /// A catalog or `catalog.schema` still worth walking.
    pub fn keeps_prefix(&self, name: &str) -> bool {
        let included =
            self.include.is_empty() || self.include.iter().any(|pattern| can_reach(name, pattern));
        included
            && !self
                .exclude
                .iter()
                .any(|pattern| prunes_prefix(name, pattern))
    }

    /// Catalogs `--include` can name without walking `/catalogs`.
    pub fn catalog_scope(&self) -> Option<Vec<String>> {
        literal_heads(&self.include, 0)
    }

    /// Schemas `--include` can name inside `catalog` without walking `/schemas`.
    pub fn schema_scope(&self, catalog: &str) -> Option<Vec<String>> {
        let patterns: Vec<String> = self
            .include
            .iter()
            .filter(|pattern| can_reach(catalog, pattern))
            .cloned()
            .collect();
        if patterns.is_empty() {
            return None;
        }
        literal_heads(&patterns, 1)
    }

    fn included(&self, name: &str) -> bool {
        self.include.is_empty()
            || self
                .include
                .iter()
                .any(|pattern| matches_fqn(name, pattern))
    }

    fn excluded(&self, name: &str) -> bool {
        self.exclude
            .iter()
            .any(|pattern| matches_fqn(name, pattern))
    }
}

pub fn is_glob(pattern: &str) -> bool {
    pattern.contains('*') || pattern.contains('?') || pattern.contains('[')
}

fn matches_fqn(name: &str, pattern: &str) -> bool {
    if is_glob(pattern) {
        if glob_matches(pattern, name) {
            return true;
        }
        return components_match(name, pattern, false);
    }
    name == pattern
        || name.starts_with(&format!("{pattern}."))
        || name.starts_with(&format!("{pattern}/"))
}

fn can_reach(prefix: &str, pattern: &str) -> bool {
    if is_glob(pattern) && glob_matches(pattern, prefix) {
        return true;
    }
    components_match(prefix, pattern, true)
}

fn prunes_prefix(prefix: &str, pattern: &str) -> bool {
    let prefix_parts = split_fqn(prefix);
    let pattern_parts = split_fqn(pattern);
    if pattern_parts.len() > prefix_parts.len() {
        return false;
    }
    matches_fqn(prefix, pattern)
}

fn components_match(name: &str, pattern: &str, prefix: bool) -> bool {
    let name_parts = split_fqn(name);
    let pattern_parts = split_fqn(pattern);
    if name_parts.is_empty() || pattern_parts.is_empty() {
        return false;
    }
    if !prefix && name_parts.len() < pattern_parts.len() {
        return false;
    }
    let shared = name_parts.len().min(pattern_parts.len());
    name_parts
        .iter()
        .zip(pattern_parts.iter())
        .take(shared)
        .all(|(name, pattern)| component_matches(name, pattern))
}

fn component_matches(name: &str, pattern: &str) -> bool {
    if is_glob(pattern) {
        glob_matches(pattern, name)
    } else {
        name == pattern
    }
}

fn glob_matches(pattern: &str, name: &str) -> bool {
    glob::Pattern::new(pattern)
        .map(|glob| glob.matches(name))
        .unwrap_or(false)
}

fn split_fqn(name: &str) -> Vec<&str> {
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
        let parts = split_fqn(pattern);
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
