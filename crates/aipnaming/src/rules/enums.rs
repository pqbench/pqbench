//! AIP-126 enum rules.

use crate::decl::DeclKind;
use crate::lint::{report, Finding, Severity};
use crate::words::split_identifier;

use super::{by_owner, Context};

/// Ported from api-linter `core::0126::unspecified`: the first value is the
/// zero value, either `UNKNOWN` or `<ENUM>_UNSPECIFIED`.
pub(super) fn unspecified(ctx: &Context<'_>, findings: &mut Vec<Finding>) {
    for (owner, variants) in by_owner(ctx.decls, DeclKind::Variant) {
        let Some(first) = variants.first() else {
            continue;
        };
        if first.name == "UNKNOWN"
            || first.name.ends_with("_UNSPECIFIED")
            || first.name.ends_with("_UNKNOWN")
        {
            continue;
        }
        let suggestion = format!("{}_UNSPECIFIED", upper_snake(owner));
        report(
            findings,
            "aip-126/unspecified",
            Severity::WARNING,
            first,
            format!("The first enum value should be `UNKNOWN` or `{suggestion}`."),
            Some(suggestion),
        );
    }
}

fn upper_snake(name: &str) -> String {
    split_identifier(name).join("_").to_ascii_uppercase()
}
