//! AIP-142 time and duration field names.

use crate::decl::{DeclKind, Declaration};
use crate::lint::{report, Finding, Severity};

use super::{lower_words, Context};

/// Ported from api-linter `core::0142::time-field-names`, plus the imperative
/// list from the AIP text (`publish_time`, not `published_time`).
const MISTAKES: &[(&str, &str)] = &[
    ("created", "create_time"),
    ("creation", "create_time"),
    ("expired", "expire_time"),
    ("modified", "update_time"),
    ("published", "publish_time"),
    ("purged", "purge_time"),
    ("updated", "update_time"),
];

pub(super) fn field_names(ctx: &Context<'_>, findings: &mut Vec<Finding>) {
    for decl in ctx.decls.iter().filter(|decl| decl.kind == DeclKind::Field) {
        let words = lower_words(&decl.name);
        let timeish = words.iter().any(|word| {
            matches!(
                word.as_str(),
                "time" | "times" | "at" | "date" | "timestamp"
            )
        }) || type_is_time(decl);
        let ends_at = decl.name.ends_with("_at");

        if timeish && !ends_at {
            for (mistake, imperative) in MISTAKES {
                if words.iter().any(|word| word == mistake) {
                    report(
                        findings,
                        "aip-142/time-field-names",
                        Severity::WARNING,
                        decl,
                        format!("Timestamps use the imperative and a `_time` suffix: prefer `{imperative}` to `{}`.", decl.name),
                        Some((*imperative).to_owned()),
                    );
                    break;
                }
            }
        }
        if ends_at {
            report(
                findings,
                "aip-142/time-field-names",
                Severity::WARNING,
                decl,
                format!(
                    "Prefer a `_time` suffix to `_at` (`create_time`, not `{}`).",
                    decl.name
                ),
                None,
            );
        }
        if words.iter().any(|word| word == "timestamp") {
            report(
                findings,
                "aip-142/time-field-names",
                Severity::WARNING,
                decl,
                "Use a `_time` suffix instead of `timestamp`.",
                None,
            );
        }
        if decl.name == "time" {
            report(
                findings,
                "aip-142/time-field-names",
                Severity::WARNING,
                decl,
                "Name a timestamp after what it marks: `create_time`, not bare `time`.",
                None,
            );
        }
    }
}

/// Ported from api-linter `core::0142::duration-offset-comment`.
pub(super) fn offset_comment(ctx: &Context<'_>, findings: &mut Vec<Finding>) {
    for decl in ctx
        .decls
        .iter()
        .filter(|decl| decl.kind == DeclKind::Field && decl.name.ends_with("_offset") && !decl.docs)
    {
        report(
            findings,
            "aip-142/duration-offset-comment",
            Severity::WARNING,
            decl,
            "Fields ending in `_offset` should document what the offset is measured from.",
            None,
        );
    }
}

fn type_is_time(decl: &Declaration) -> bool {
    matches!(
        decl.type_constructor_name(),
        Some(
            "SystemTime"
                | "Instant"
                | "Duration"
                | "DateTime"
                | "NaiveDateTime"
                | "OffsetDateTime"
                | "StdDuration"
                | "Timestamp"
        )
    )
}
