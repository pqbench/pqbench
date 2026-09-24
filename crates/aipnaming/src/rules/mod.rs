//! The rule set.
//!
//! Rule ids mirror Google's `api-linter`, which applies the same AIP rules to
//! protobuf descriptors: `<aip-proposal>/<rule>`. The Rust adaptations and
//! their limits are documented at each check.

use crate::decl::{DeclKind, Declaration};
use crate::lint::Finding;

pub(crate) mod naming;
pub(crate) mod plural;
pub(crate) mod ranges;
pub(crate) mod spelling;
pub(crate) mod time;
pub(crate) mod units;

/// What a rule sees: the declarations of one file.
pub(crate) struct Context<'a> {
    pub decls: &'a [Declaration],
}

/// A single rule: a stable id and a check that pushes findings.
pub(crate) struct Rule {
    pub id: &'static str,
    pub check: fn(&Context<'_>, &mut Vec<Finding>),
}

/// Every rule, in report order.
pub(crate) const RULES: &[Rule] = &[
    Rule {
        id: "aip-190/casing",
        check: naming::casing,
    },
    Rule {
        id: "aip-140/underscores",
        check: naming::underscores,
    },
    Rule {
        id: "aip-140/abbreviations",
        check: naming::abbreviations,
    },
    Rule {
        id: "aip-140/prepositions",
        check: naming::prepositions,
    },
    Rule {
        id: "aip-136/method-prepositions",
        check: naming::method_prepositions,
    },
    Rule {
        id: "aip-140/verbs",
        check: naming::verbs,
    },
    Rule {
        id: "aip-140/booleans",
        check: naming::booleans,
    },
    Rule {
        id: "aip-140/reserved-words",
        check: naming::reserved_words,
    },
    Rule {
        id: "aip-136/async-name",
        check: naming::async_name,
    },
    Rule {
        id: "aip-140/plural",
        check: plural::agreement,
    },
    Rule {
        id: "aip-141/units",
        check: units::units,
    },
    Rule {
        id: "aip-190/american-english",
        check: spelling::american_english,
    },
    Rule {
        id: "aip-142/time-field-names",
        check: time::field_names,
    },
    Rule {
        id: "aip-142/duration-offset-comment",
        check: time::offset_comment,
    },
    Rule {
        id: "aip-145/ranges",
        check: ranges::first_last,
    },
];

/// The lower-cased words of an identifier.
pub(crate) fn lower_words(name: &str) -> Vec<String> {
    crate::words::split_identifier(name)
        .into_iter()
        .map(|word| word.to_ascii_lowercase())
        .collect()
}

/// Declarations of one kind, grouped by owner in first-seen order.
pub(crate) fn by_owner(decls: &[Declaration], kind: DeclKind) -> Vec<(&str, Vec<&Declaration>)> {
    let mut groups: Vec<(&str, Vec<&Declaration>)> = Vec::new();
    for decl in decls.iter().filter(|decl| decl.kind == kind) {
        let Some(owner) = decl.owner.as_deref() else {
            continue;
        };
        match groups.iter_mut().find(|(name, _)| *name == owner) {
            Some((_, group)) => group.push(decl),
            None => groups.push((owner, vec![decl])),
        }
    }
    groups
}
