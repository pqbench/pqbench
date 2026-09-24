//! AIP-140 singular/plural agreement, scored with the word layer.
//!
//! The research is explicit that plurals correlate with collections without
//! being a certainty (member data is often plural without being repeated), so
//! the check only fires on clear cases: a known sequence type with a singular
//! head, or a plain singular type with a regular plural head. Opaque wrappers
//! (`Option`, maps, sets) and uncountable words stay quiet.

use crate::decl::{DeclKind, Declaration};
use crate::lint::{report, Finding, Severity};
use crate::words::{is_plural, is_singular, split_identifier};

use super::Context;

/// Types that hold zero or more elements of the named kind.
const SEQUENCES: &[&str] = &[
    "Vec",
    "VecDeque",
    "BinaryHeap",
    "SmallVec",
    "ArrayVec",
    "LinkedList",
];

/// Wrappers whose shape does not decide singular vs plural.
const OPAQUE: &[&str] = &[
    "Option",
    "Result",
    "Cow",
    "Box",
    "Rc",
    "Arc",
    "RefCell",
    "Cell",
    "Mutex",
    "RwLock",
    "HashMap",
    "BTreeMap",
    "IndexMap",
    "HashSet",
    "BTreeSet",
    "IndexSet",
    "PhantomData",
];

/// Scalar types where a plural name is a quantity (`total_bytes: u64`), not a
/// repeated field.
const SCALARS: &[&str] = &[
    "bool", "char", "str", "String", "OsStr", "f32", "f64", "u8", "u16", "u32", "u64", "u128",
    "usize", "i8", "i16", "i32", "i64", "i128", "isize",
];

pub(super) fn agreement(ctx: &Context<'_>, findings: &mut Vec<Finding>) {
    for field in ctx.decls.iter().filter(|decl| decl.kind == DeclKind::Field) {
        let Some(head) = split_identifier(&field.name)
            .last()
            .map(|word| word.to_ascii_lowercase())
        else {
            continue;
        };
        if is_sequence(field) {
            if is_singular(&head) {
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
        return SEQUENCES.contains(&constructor);
    }
    field
        .type_name
        .as_deref()
        .is_some_and(|ty| ty.trim_start_matches('&').trim_start().starts_with('['))
}

fn is_opaque(field: &Declaration) -> bool {
    field
        .type_constructor_name()
        .is_some_and(|constructor| OPAQUE.contains(&constructor))
}

fn is_scalar(field: &Declaration) -> bool {
    field
        .type_name
        .as_deref()
        .is_some_and(|ty| SCALARS.contains(&ty.trim()))
}
