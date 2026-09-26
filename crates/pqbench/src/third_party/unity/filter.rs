//! Filter helpers the catalog walk uses: prefix reach, scope, and pruning.
//!
//! These complement [`NameFilter::keeps`] for the partial (`catalog`,
//! `catalog.schema`) names the crawl visits before a full table FQN exists.

use crate::third_party::unity::api::{self, NameFilter};

/// A catalog or `catalog.schema` still worth walking.
pub(super) fn keeps_prefix(filter: &NameFilter, name: &str) -> bool {
    filter.keeps_prefix(name)
}

/// Catalogs `--include` can name without walking `/catalogs`.
pub(super) fn catalog_scope(filter: &NameFilter) -> Option<Vec<String>> {
    literal_heads(&filter.include, 0)
}

/// Schemas `--include` can name inside `catalog` without walking `/schemas`.
pub(super) fn schema_scope(filter: &NameFilter, catalog: &str) -> Option<Vec<String>> {
    let patterns: Vec<String> = filter
        .include
        .iter()
        .filter(|pattern| api::can_reach(catalog, pattern))
        .cloned()
        .collect();
    if patterns.is_empty() {
        return None;
    }
    literal_heads(&patterns, 1)
}

fn literal_heads(patterns: &[String], index: usize) -> Option<Vec<String>> {
    if patterns.is_empty() {
        return None;
    }
    let mut heads = Vec::new();
    for pattern in patterns {
        let parts = api::split_fqn(pattern);
        let Some(part) = parts.get(index) else {
            continue;
        };
        if api::is_glob(part) {
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
