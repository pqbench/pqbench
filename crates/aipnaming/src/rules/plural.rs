//! AIP-140 singular/plural agreement, scored with the word layer.
//!
//! The research is explicit that plurals correlate with collections without
//! being a certainty (member data is often plural without being repeated), so
//! the check only fires on clear cases: a known sequence type with a singular
//! head, or a plain singular type with a regular plural head. Opaque wrappers
//! (`Option`, maps, sets) and uncountable words stay quiet.

use crate::data;
use crate::decl::{DeclKind, Declaration};
use crate::lint::{report, Finding, Severity};
use crate::words::{classify, is_plural, is_singular, split_identifier, WordKind};

use super::Context;

pub(super) fn agreement(ctx: &Context<'_>, findings: &mut Vec<Finding>) {
    for field in ctx.decls.iter().filter(|decl| decl.kind == DeclKind::Field) {
        let Some(head) = split_identifier(&field.name)
            .last()
            .map(|word| word.to_ascii_lowercase())
        else {
            continue;
        };
        // `log: Vec<LogCommit>` and other collective nouns read the same in
        // both numbers, and `options: Options` follows its type name; both are
        // legitimate, not agreement errors.
        if is_uncountable(&head) || matches_type_name(field, &head) {
            continue;
        }
        if is_sequence(field) {
            if is_singular(&head) && classify(&head) != WordKind::Verb {
                report(
                    findings,
                    "aip-140/plural",
                    Severity::WARNING,
                    field,
                    format!(
                        "Sequence fields use the plural form (`books`, not `{}`).",
                        field.name
                    ),
                    None,
                );
            }
            continue;
        }
        let scalar = field.type_name.is_some()
            && field.type_constructor().is_some()
            && !is_opaque(field)
            && !is_scalar(field);
        if scalar && is_plural(&head) {
            report(
                findings,
                "aip-140/plural",
                Severity::WARNING,
                field,
                format!(
                    "Non-repeated fields use the singular form (`book`, not `{}`).",
                    field.name
                ),
                None,
            );
        }
    }
}

fn is_sequence(field: &Declaration) -> bool {
    if let Some(constructor) = field.type_constructor_name() {
        return data::set("sequence-types").contains(constructor);
    }
    field
        .type_name
        .as_deref()
        .is_some_and(|ty| ty.trim_start_matches('&').trim_start().starts_with('['))
}

fn is_opaque(field: &Declaration) -> bool {
    field
        .type_constructor_name()
        .is_some_and(|constructor| data::set("opaque-types").contains(constructor))
}

fn is_uncountable(head: &str) -> bool {
    data::set("uncountables").contains(head)
}

/// Whether the field shares its name with its type (`options: Options`): the
/// field then follows the type name rather than describing a collection.
fn matches_type_name(field: &Declaration, head: &str) -> bool {
    field
        .type_constructor_name()
        .is_some_and(|name| name.eq_ignore_ascii_case(head))
}

fn is_scalar(field: &Declaration) -> bool {
    field
        .type_name
        .as_deref()
        .is_some_and(|ty| data::set("scalar-types").contains(ty.trim()))
}
