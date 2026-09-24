//! AIP-141 quantities and units, spelled the way this workspace writes them.

use crate::decl::DeclKind;
use crate::lint::{report, Finding, Severity};
use crate::words::{singular, split_identifier};

use super::Context;

/// Bit/byte rate abbreviations that must be spelled out.
const RATE_TOKENS: &[&str] = &["mbps", "kbps", "gbps", "tbps"];
/// Unit abbreviations that must not precede `per`.
const UNIT_TOKENS: &[&str] = &["mb", "kb", "gb", "tb", "mib", "kib", "gib", "tib"];
/// Legacy integer time units per AIP-142: spell them out.
const LEGACY_TIME_UNITS: &[&str] = &["ms", "sec", "secs", "min", "mins", "hr", "hrs"];

pub(super) fn units(ctx: &Context<'_>, findings: &mut Vec<Finding>) {
    for decl in ctx.decls.iter().filter(|decl| decl.kind == DeclKind::Field) {
        let words: Vec<String> = split_identifier(&decl.name)
            .iter()
            .map(|word| word.to_ascii_lowercase())
            .collect();
        for (index, word) in words.iter().enumerate() {
            if RATE_TOKENS.contains(&word.as_str()) {
                report(
                    findings,
                    "aip-141/units",
                    Severity::WARNING,
                    decl,
                    "Spell units out: `megabytes_per_second`, not `mbps` or `mb_per_s`.",
                    None,
                );
            }
            if UNIT_TOKENS.contains(&word.as_str())
                && words.get(index + 1).is_some_and(|next| next == "per")
            {
                report(
                    findings,
                    "aip-141/units",
                    Severity::WARNING,
                    decl,
                    "Spell units out: `megabytes_per_second`, not `mb_per_s`.",
                    None,
                );
            }
        }
        if decl.name.starts_with("num_") && words.len() > 1 {
            let suggestion = format!("{}_count", singular(&words[1..].join("_")));
            report(
                findings,
                "aip-141/count-suffix",
                Severity::WARNING,
                decl,
                "Quantities use a `_count` suffix, not a `num_` prefix.",
                Some(suggestion),
            );
        }
        if words.iter().any(|word| word == "measurement") {
            report(
                findings,
                "aip-141/units",
                Severity::WARNING,
                decl,
                "Use `_estimate` for a measured value, not `_measurement`.",
                Some(decl.name.replace("measurement", "estimate")),
            );
        }
        if is_integer(decl)
            && words
                .last()
                .is_some_and(|word| LEGACY_TIME_UNITS.contains(&word.as_str()))
        {
            report(
                findings,
                "aip-142/time-field-names",
                Severity::WARNING,
                decl,
                "Spell integer time units out: `send_time_millis`, not `_ms`.",
                None,
            );
        }
        if is_duration(decl) && words.last().is_some_and(|word| word == "times") {
            report(
                findings,
                "aip-142/time-field-names",
                Severity::WARNING,
                decl,
                "Spans end in `_durations`, not `_times` (`read_durations`).",
                Some(format!("{}_durations", words[..words.len() - 1].join("_"))),
            );
        }
    }
}

fn is_integer(decl: &crate::decl::Declaration) -> bool {
    matches!(
        decl.type_name.as_deref(),
        Some(
            "u8" | "u16"
                | "u32"
                | "u64"
                | "u128"
                | "usize"
                | "i8"
                | "i16"
                | "i32"
                | "i64"
                | "i128"
                | "isize"
        )
    )
}

fn is_duration(decl: &crate::decl::Declaration) -> bool {
    matches!(
        decl.type_constructor_name(),
        Some("Duration" | "StdDuration")
    )
}
