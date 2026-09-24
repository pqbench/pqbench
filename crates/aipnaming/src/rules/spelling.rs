//! AIP-190: names use correct American English.

use crate::lint::{report, Finding, Severity};
use crate::words::split_identifier;

use super::Context;

/// The high-confidence American/British pairs seen in code: spelling variants
/// with no sense-dependent meaning (`licence`/`license` is deliberately absent;
/// the noun/verb split makes it unsafe to rewrite).
const AMERICAN: &[(&str, &str)] = &[
    ("aluminium", "aluminum"),
    ("analyse", "analyze"),
    ("analysed", "analyzed"),
    ("analyses", "analyzes"),
    ("analysing", "analyzing"),
    ("artefact", "artifact"),
    ("behaviour", "behavior"),
    ("behaviours", "behaviors"),
    ("cancelled", "canceled"),
    ("cancelling", "canceling"),
    ("catalogue", "catalog"),
    ("catalogued", "cataloged"),
    ("catalogues", "catalogs"),
    ("centres", "centers"),
    ("centred", "centered"),
    ("centrepiece", "centerpiece"),
    ("colour", "color"),
    ("colours", "colors"),
    ("coloured", "colored"),
    ("defence", "defense"),
    ("dialogue", "dialog"),
    ("favour", "favor"),
    ("favours", "favors"),
    ("favoured", "favored"),
    ("fibre", "fiber"),
    ("fibres", "fibers"),
    ("finalise", "finalize"),
    ("finalised", "finalized"),
    ("grey", "gray"),
    ("honour", "honor"),
    ("initialise", "initialize"),
    ("initialised", "initialized"),
    ("initialisation", "initialization"),
    ("labelled", "labeled"),
    ("labelling", "labeling"),
    ("licence", "license"),
    ("litre", "liter"),
    ("litres", "liters"),
    ("metre", "meter"),
    ("metres", "meters"),
    ("modelled", "modeled"),
    ("modelling", "modeling"),
    ("normalise", "normalize"),
    ("normalised", "normalized"),
    ("normalisation", "normalization"),
    ("offence", "offense"),
    ("optimise", "optimize"),
    ("optimised", "optimized"),
    ("optimisation", "optimization"),
    ("organise", "organize"),
    ("organised", "organized"),
    ("organisation", "organization"),
    ("recognise", "recognize"),
    ("recognised", "recognized"),
    ("sceptic", "skeptic"),
    ("serialise", "serialize"),
    ("serialised", "serialized"),
    ("serialisation", "serialization"),
    ("theatre", "theater"),
    ("travelled", "traveled"),
    ("travelling", "traveling"),
    ("utilise", "utilize"),
    ("utilised", "utilized"),
];

pub(super) fn american_english(ctx: &Context<'_>, findings: &mut Vec<Finding>) {
    for decl in ctx.decls {
        for word in split_identifier(&decl.name) {
            let lower = word.to_ascii_lowercase();
            let Some((_, american)) = AMERICAN.iter().find(|(british, _)| *british == lower) else {
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
