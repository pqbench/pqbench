//! AIP-145 range field names: `first`/`last`, not `min`/`max`.

use std::collections::BTreeMap;

use crate::decl::{DeclKind, Declaration};
use crate::lint::{report, Finding, Severity};
use crate::words::split_identifier;

use super::{collect_owner_map, Context};

/// A field pair like `min_level`/`max_level` reads as a range; AIP-145 names
/// its bounds `first`/`last`.
pub(super) fn first_last(ctx: &Context<'_>, findings: &mut Vec<Finding>) {
    for (_, fields) in collect_owner_map(ctx.decls, DeclKind::Field) {
        let mut mins: BTreeMap<String, &Declaration> = BTreeMap::new();
        let mut maxs: BTreeMap<String, &Declaration> = BTreeMap::new();
        for field in fields {
            let words: Vec<String> = split_identifier(&field.name)
                .iter()
                .map(|word| word.to_ascii_lowercase())
                .collect();
            if let Some(key) = strip_marker(&words, "min") {
                mins.insert(key, field);
            }
            if let Some(key) = strip_marker(&words, "max") {
                maxs.insert(key, field);
            }
        }
        for (key, min_field) in &mins {
            let Some(max_field) = maxs.get(key) else {
                continue;
            };
            let label = if key.is_empty() {
                "value"
            } else {
                key.as_str()
            };
            let suggestion = format!("`first_{label}`, `last_{label}`");
            for field in [min_field, max_field] {
                report(
                    findings,
                    "aip-145/ranges",
                    Severity::WARNING,
                    field,
                    format!("Ranges use `first`/`last`, not `min`/`max` ({suggestion})."),
                    None,
                );
            }
        }
    }
}

/// The remaining words after removing the `min`/`max` marker, or `None` when
/// the marker is absent.
fn strip_marker(words: &[String], marker: &str) -> Option<String> {
    let index = words.iter().position(|word| word == marker)?;
    let rest: Vec<&str> = words
        .iter()
        .enumerate()
        .filter(|(position, _)| *position != index)
        .map(|(_, word)| word.as_str())
        .collect();
    Some(rest.join("_"))
}
