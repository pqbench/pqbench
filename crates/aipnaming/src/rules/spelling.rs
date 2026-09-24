//! AIP-190: names use correct American English.

use crate::data;
use crate::lint::{report, Finding, Severity};
use crate::words::split_identifier;

use super::Context;

pub(super) fn american_english(ctx: &Context<'_>, findings: &mut Vec<Finding>) {
    let american_pairs = data::pairs("british-american");
    for decl in ctx.decls {
        for word in split_identifier(&decl.name) {
            let lower = word.to_ascii_lowercase();
            let Some((_, american)) = american_pairs.iter().find(|(british, _)| *british == lower)
            else {
                continue;
            };
            let suggestion = decl.name.replacen(word, &match_case(word, american), 1);
            report(
                findings,
                "aip-190/american-english",
                Severity::WARNING,
                decl,
                format!("Use American English: `{american}`, not `{lower}`."),
                Some(suggestion),
            );
        }
    }
}

fn match_case(word: &str, american: &str) -> String {
    if word.chars().all(|c| c.is_ascii_uppercase()) {
        american.to_ascii_uppercase()
    } else if word.chars().next().is_some_and(|c| c.is_ascii_uppercase()) {
        let mut chars = american.chars();
        chars
            .next()
            .map(|first| first.to_ascii_uppercase().to_string() + chars.as_str())
            .unwrap_or_default()
    } else {
        american.to_owned()
    }
}
