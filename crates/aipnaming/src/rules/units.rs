//! AIP-141 quantities and units, spelled the way this workspace writes them.

use crate::data;
use crate::decl::DeclKind;
use crate::lint::{report, Finding, Severity};
use crate::words::{singular, split_identifier};

use super::Context;

pub(super) fn units(ctx: &Context<'_>, findings: &mut Vec<Finding>) {
    let rate_tokens = data::set("rate-tokens");
    let unit_tokens = data::set("unit-tokens");
    let legacy_time_units = data::set("legacy-time-units");
    for decl in ctx.decls.iter().filter(|decl| decl.kind == DeclKind::Field) {
        let words: Vec<String> = split_identifier(&decl.name)
            .iter()
            .map(|word| word.to_ascii_lowercase())
            .collect();
        for (index, word) in words.iter().enumerate() {
            if rate_tokens.contains(word.as_str()) {
                report(
                    findings,
                    "aip-141/units",
                    Severity::WARNING,
                    decl,
                    "Spell units out: `megabytes_per_second`, not `mbps` or `mb_per_s`.",
                    None,
                );
            }
            if unit_tokens.contains(word.as_str())
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
                .is_some_and(|word| legacy_time_units.contains(word.as_str()))
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
    decl.type_name
        .as_deref()
        .is_some_and(|ty| data::set("integer-types").contains(ty))
}

fn is_duration(decl: &crate::decl::Declaration) -> bool {
    decl.type_constructor_name()
        .is_some_and(|name| data::set("duration-types").contains(name))
}
