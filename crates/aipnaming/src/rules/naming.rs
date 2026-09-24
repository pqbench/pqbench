//! Name-shape rules: casing, underscores, prepositions, verbs, booleans,
//! abbreviations, and reserved words.
//!
//! These are seeded from Google's `api-linter` `aip0140` rules; the Rust
//! additions are noted per check.

use crate::data;
use crate::decl::DeclKind;
use crate::lint::{report, Finding, Severity};
use crate::words::{imperative_verb, split_identifier};

use super::Context;

/// Casing per Rust RFC 430, with the AIP-126 enum-value style accepted
/// alongside it.
///
/// AIP-126 asks for `UPPER_SNAKE_CASE` enum values; existing Rust code (and
/// this workspace's own crates) use `UpperCamelCase`, and protobuf's rationale
/// for the rule — hoisted values in generated namespaces — does not apply to
/// Rust enums. Both readings are accepted so the rule never lies about a
/// deliberate, working convention.
pub(super) fn casing(ctx: &Context<'_>, findings: &mut Vec<Finding>) {
    for decl in ctx.decls {
        let (ok, expected) = match decl.kind {
            DeclKind::Struct
            | DeclKind::Enum
            | DeclKind::Union
            | DeclKind::Trait
            | DeclKind::Alias => (is_upper_camel(&decl.name), "UpperCamelCase"),
            DeclKind::Function
            | DeclKind::Method
            | DeclKind::AssociatedFunction
            | DeclKind::Field
            | DeclKind::Module
            | DeclKind::Macro => (is_snake_case(&decl.name), "snake_case"),
            DeclKind::Constant | DeclKind::Static => {
                (is_upper_snake(&decl.name), "UPPER_SNAKE_CASE")
            }
            DeclKind::Variant => (
                is_upper_camel(&decl.name) || is_upper_snake(&decl.name),
                "UpperCamelCase or UPPER_SNAKE_CASE",
            ),
        };
        if !ok {
            report(
                findings,
                "aip-190/casing",
                Severity::ERROR,
                decl,
                format!("`{}` should use {expected}.", decl.name),
                None,
            );
        }
    }
}

/// Ported from api-linter `core::0140::underscores`: no leading, trailing, or
/// adjacent underscores. Fields only, matching the linter's scope.
pub(super) fn underscores(ctx: &Context<'_>, findings: &mut Vec<Finding>) {
    for decl in ctx.decls.iter().filter(|decl| decl.kind == DeclKind::Field) {
        if decl.name.starts_with('_') || decl.name.ends_with('_') || decl.name.contains("__") {
            report(
                findings,
                "aip-140/underscores",
                Severity::WARNING,
                decl,
                format!(
                    "`{}` must not begin or end with an underscore, or use adjacent underscores.",
                    decl.name
                ),
                None,
            );
        }
    }
}

/// Ported from api-linter `core::0140::abbreviations`, with the Rust-local
/// `cfg` -> `config` added: AIP-140 names `config` the well-known abbreviation.
pub(super) fn abbreviations(ctx: &Context<'_>, findings: &mut Vec<Finding>) {
    let abbreviations = data::pairs("abbreviations");
    let local = data::pairs("local-abbreviations");
    for decl in ctx.decls {
        for word in split_identifier(&decl.name) {
            let lower = word.to_ascii_lowercase();
            if let Some((long, short)) = abbreviations.iter().find(|(long, _)| *long == lower) {
                let suggestion = decl.name.replacen(word, short, 1);
                report(
                    findings,
                    "aip-140/abbreviations",
                    Severity::WARNING,
                    decl,
                    format!("Use the common abbreviation `{short}` instead of `{long}`."),
                    Some(format!("`{suggestion}`")),
                );
            }
            if let Some((local, standard)) = local.iter().find(|(local, _)| *local == lower) {
                let suggestion = decl.name.replacen(word, standard, 1);
                report(
                    findings,
                    "aip-140/abbreviations",
                    Severity::WARNING,
                    decl,
                    format!("Use `{standard}` rather than the abbreviation `{local}`."),
                    Some(format!("`{suggestion}`")),
                );
            }
        }
    }
}

/// Ported from api-linter `core::0140::prepositions`: field names avoid
/// prepositions. Method names are the sibling rule below.
pub(super) fn prepositions(ctx: &Context<'_>, findings: &mut Vec<Finding>) {
    check_prepositions(ctx, findings, &[DeclKind::Field]);
}

/// AIP-136 read of the same examples for function and type names. Test
/// functions and test modules are skipped: they are documentation of behavior,
/// not API surface. Conversion prefixes `from_`/`to_`/`into_`/`as_` are Rust's
/// required idiom and stay allowed.
pub(super) fn method_prepositions(ctx: &Context<'_>, findings: &mut Vec<Finding>) {
    const KINDS: &[DeclKind] = &[
        DeclKind::Function,
        DeclKind::Method,
        DeclKind::AssociatedFunction,
        DeclKind::Struct,
        DeclKind::Enum,
        DeclKind::Union,
        DeclKind::Trait,
        DeclKind::Alias,
    ];
    check_prepositions(ctx, findings, KINDS);
}

fn check_prepositions(ctx: &Context<'_>, findings: &mut Vec<Finding>, kinds: &[DeclKind]) {
    const CONVERSION_PREFIXES: &[&str] = &["from", "to", "into", "as"];
    for decl in ctx.decls.iter().filter(|decl| kinds.contains(&decl.kind)) {
        if decl.kind == DeclKind::Field
            && data::set("field-exceptions").contains(&decl.name.as_str())
        {
            continue;
        }
        if decl.test {
            continue;
        }
        for (index, word) in split_identifier(&decl.name).iter().enumerate() {
            let lower = word.to_ascii_lowercase();
            if !crate::words::classify(&lower).eq(&crate::words::WordKind::Preposition) {
                continue;
            }
            if index == 0 && CONVERSION_PREFIXES.contains(&lower.as_str()) {
                continue;
            }
            report(
                findings,
                preposition_rule(decl.kind),
                Severity::WARNING,
                decl,
                format!("Avoid using `{lower}` in {} names.", kind_noun(decl.kind)),
                None,
            );
        }
    }
}

fn preposition_rule(kind: DeclKind) -> &'static str {
    match kind {
        DeclKind::Field => "aip-140/prepositions",
        _ => "aip-136/method-prepositions",
    }
}

/// AIP-140: fields state what is, not what to do. Flags a bare imperative verb
/// (`disable`) or a verb followed by its object (`collect_items`); a
/// verb/noun like `compress_durations` reads as state and stays quiet, since
/// the same letters can be an attributive noun.
pub(super) fn verbs(ctx: &Context<'_>, findings: &mut Vec<Finding>) {
    for decl in ctx.decls.iter().filter(|decl| decl.kind == DeclKind::Field) {
        let words = split_identifier(&decl.name);
        let Some(first) = words.first() else {
            continue;
        };
        let first = first.to_ascii_lowercase();
        if !imperative_verb(&first) {
            continue;
        }
        let participle = participle(&first);
        let suggestion = if words.len() == 1 {
            participle
        } else {
            format!("{participle}_{}", words[1..].join("_").to_ascii_lowercase())
        };
        report(
            findings,
            "aip-140/verbs",
            Severity::WARNING,
            decl,
            format!(
                "Fields describe state: prefer `{suggestion}` to `{}`.",
                decl.name
            ),
            Some(suggestion),
        );
    }
}

/// AIP-140: booleans omit the verb prefix. Needs the written `bool` type; an
/// alias or `Option<bool>` is left alone.
pub(super) fn booleans(ctx: &Context<'_>, findings: &mut Vec<Finding>) {
    const PREFIXES: &[&str] = &[
        "is", "has", "can", "should", "was", "were", "does", "did", "will", "would",
    ];
    for decl in ctx.decls.iter().filter(|decl| decl.kind == DeclKind::Field) {
        if !decl.is_bool() {
            continue;
        }
        let Some((prefix, stripped)) = decl.name.split_once('_') else {
            continue;
        };
        if !PREFIXES.contains(&prefix) || stripped.is_empty() {
            continue;
        }
        if reserved_word(stripped) {
            continue;
        }
        report(
            findings,
            "aip-140/booleans",
            Severity::ERROR,
            decl,
            format!(
                "Booleans omit the verb prefix: use `{stripped}`, not `{}`.",
                decl.name
            ),
            Some(stripped.to_owned()),
        );
    }
}

/// Ported from api-linter `core::0140::reserved-words`, applied to fields.
pub(super) fn reserved_words(ctx: &Context<'_>, findings: &mut Vec<Finding>) {
    for decl in ctx.decls.iter().filter(|decl| decl.kind == DeclKind::Field) {
        if reserved_word(&decl.name) {
            report(
                findings,
                "aip-140/reserved-words",
                Severity::WARNING,
                decl,
                format!(
                    "`{}` is a reserved word in a common language and should not be used.",
                    decl.name
                ),
                None,
            );
        }
    }
}

/// AIP-136: names never contain `async`; use `LongRunning` when an immediate
/// and a long-running variant must be told apart.
pub(super) fn async_name(ctx: &Context<'_>, findings: &mut Vec<Finding>) {
    for decl in ctx.decls.iter().filter(|decl| {
        matches!(
            decl.kind,
            DeclKind::Function | DeclKind::Method | DeclKind::AssociatedFunction
        ) && !decl.test
    }) {
        if split_identifier(&decl.name)
            .iter()
            .any(|word| word.eq_ignore_ascii_case("async"))
        {
            report(
                findings,
                "aip-136/async-name",
                Severity::ERROR,
                decl,
                "Names never contain `async`; use `LongRunning` if two variants must be told apart.",
                None,
            );
        }
    }
}

fn reserved_word(name: &str) -> bool {
    data::set("reserved-words").contains(name)
}

fn kind_noun(kind: DeclKind) -> &'static str {
    match kind {
        DeclKind::Field => "field",
        DeclKind::Variant => "enum variant",
        DeclKind::Constant | DeclKind::Static => "constant",
        DeclKind::Struct | DeclKind::Enum | DeclKind::Union | DeclKind::Trait | DeclKind::Alias => {
            "type"
        }
        DeclKind::Module => "module",
        DeclKind::Macro => "macro",
        _ => "function",
    }
}

fn participle(verb: &str) -> String {
    if let Some(stem) = verb.strip_suffix('e') {
        return format!("{stem}ed");
    }
    if verb.len() > 1 && verb.ends_with('y') {
        return format!("{}ied", &verb[..verb.len() - 1]);
    }
    format!("{verb}ed")
}

fn is_upper_camel(name: &str) -> bool {
    !name.is_empty()
        && name.chars().next().is_some_and(|c| c.is_ascii_uppercase())
        && name.chars().all(|c| c.is_ascii_alphanumeric())
}

fn is_snake_case(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        && name.chars().any(|c| c.is_ascii_alphabetic())
}

fn is_upper_snake(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
        && name.chars().any(|c| c.is_ascii_alphabetic())
}
