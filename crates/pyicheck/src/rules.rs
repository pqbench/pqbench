//! The rule set: checks that compare the two surfaces, and the models behind
//! them.
//!
//! The stub is the hand-maintained mirror, so a finding points at the stub —
//! the fix goes there — and names what the bindings say. A name the stub
//! declares and the bindings do not points at the stub too: the stub is the
//! artifact a reader trusts.

use std::collections::BTreeSet;

use crate::lint::{Reporter, Severity};
use crate::model::{Entry, Field, Kind, ModelField, ModelIndex, Param, Surface};

/// What a rule sees: both surfaces and the library models.
pub(crate) struct Inputs<'a> {
    /// The bindings, one surface per source file.
    pub bindings: &'a [Surface],
    /// The stub.
    pub stub: &'a Surface,
    /// The library's `pub struct` models, for the field rule.
    pub model: &'a ModelIndex,
}

impl Inputs<'_> {
    /// The binding entry at a path, with the file it came from.
    pub(crate) fn binding(&self, path: &str) -> Option<(&Surface, &Entry)> {
        self.bindings
            .iter()
            .find_map(|surface| surface.get(path).map(|entry| (surface, entry)))
    }

    /// Every binding entry, with its file, of a kind.
    fn binding_entries(&self, kind: Kind) -> impl Iterator<Item = (&Surface, &Entry)> {
        self.bindings.iter().flat_map(move |surface| {
            surface
                .entries
                .iter()
                .filter(move |entry| entry.kind == kind)
                .map(move |entry| (surface, entry))
        })
    }

    /// The stub class at a path.
    fn stub_class(&self, path: &str) -> Option<&Entry> {
        self.stub
            .get(path)
            .filter(|entry| entry.kind == Kind::Class)
    }

    /// The stub class named `name`, wherever it is declared.
    fn stub_class_named(&self, name: &str) -> Option<&Entry> {
        self.stub
            .entries
            .iter()
            .find(|entry| entry.kind == Kind::Class && entry.name() == name)
    }
}

/// A single rule: a stable id, its severity, and a check.
pub(crate) struct Rule {
    pub id: &'static str,
    pub severity: Severity,
    pub check: fn(&Inputs<'_>, &mut Reporter<'_>),
}

/// Every rule, in report order.
pub(crate) const RULES: &[Rule] = &[
    Rule {
        id: "surface/parity",
        severity: Severity::ERROR,
        check: parity,
    },
    Rule {
        id: "surface/params",
        severity: Severity::ERROR,
        check: params,
    },
    Rule {
        id: "surface/returns",
        severity: Severity::ERROR,
        check: returns,
    },
    Rule {
        id: "surface/method",
        severity: Severity::WARNING,
        check: method,
    },
    Rule {
        id: "model/base",
        severity: Severity::ERROR,
        check: base,
    },
    Rule {
        id: "model/field",
        severity: Severity::WARNING,
        check: field,
    },
];

/// Every name on one side exists on the other.
///
/// The stub writes a module as a nested class and may hang an export on that
/// class as an annotation instead of a nested class (`module.Class`), so
/// a binding module is satisfied by a stub class of the same path, and a
/// binding name by a member of its parent's proxy class.
fn parity(inputs: &Inputs<'_>, reporter: &mut Reporter<'_>) {
    for surface in inputs.bindings {
        for binding in &surface.entries {
            check_binding(inputs, surface, binding, reporter);
        }
    }
    for stub in &inputs.stub.entries {
        if inputs.binding(&stub.path).is_none() {
            reporter.report(
                &inputs.stub.file,
                stub,
                format!("the bindings expose no `{}`", stub.path),
            );
        }
    }
}

/// One binding name: the stub mirrors it, or the finding goes to the side that
/// has it — the stub when the kinds disagree, the binding when the stub omits
/// the name outright.
fn check_binding(
    inputs: &Inputs<'_>,
    surface: &Surface,
    binding: &Entry,
    reporter: &mut Reporter<'_>,
) {
    match inputs.stub.get(&binding.path) {
        Some(stub) if compatible(binding.kind, stub.kind) => {}
        Some(stub) => reporter.report(
            &inputs.stub.file,
            stub,
            format!(
                "the bindings expose {} as a {}, the stub as a {}",
                binding.path,
                binding.kind.as_str(),
                stub.kind.as_str()
            ),
        ),
        None if member(inputs.stub, &binding.path) => {}
        None => reporter.report(
            &surface.file,
            binding,
            format!("the stub declares no `{}`", binding.path),
        ),
    }
}

/// A function's parameters agree, name for name and default for default.
fn params(inputs: &Inputs<'_>, reporter: &mut Reporter<'_>) {
    for (_, binding) in inputs.binding_entries(Kind::Function) {
        let Some(stub) = inputs.function(&binding.path) else {
            continue;
        };
        if same_params(&binding.params, &stub.params) {
            continue;
        }
        reporter.report(
            &inputs.stub.file,
            stub,
            format!(
                "the bindings take ({}), the stub takes ({})",
                render_params(&binding.params),
                render_params(&stub.params)
            ),
        );
    }
}

/// A function's return shape and awaitability agree. The wrapper the binding
/// calls (`one`, `many`, `stream_listing`, `document`) is the only statement of
/// them, and the stub is where they are written down.
fn returns(inputs: &Inputs<'_>, reporter: &mut Reporter<'_>) {
    for (_, binding) in inputs.binding_entries(Kind::Function) {
        let Some(stub) = inputs.function(&binding.path) else {
            continue;
        };
        if let Some(expected) = binding.returns.as_deref() {
            let mismatch = match stub.returns.as_deref() {
                None => Some(format!(
                    "the bindings return `{expected}`, the stub declares no return"
                )),
                Some(declared) if canonical(expected) != canonical(declared) => Some(format!(
                    "the bindings return `{expected}`, the stub declares `{declared}`"
                )),
                Some(_) => None,
            };
            if let Some(message) = mismatch {
                reporter.report(&inputs.stub.file, stub, message);
            }
        }
        if let Some(expected) = binding.is_async {
            if stub.is_async != Some(expected) {
                let (binding_word, stub_word) = if expected {
                    ("awaitable", "not awaitable")
                } else {
                    ("not awaitable", "awaitable")
                };
                reporter.report(
                    &inputs.stub.file,
                    stub,
                    format!("the binding call is {binding_word}, the stub declares it {stub_word}"),
                );
            }
        }
    }
}

/// A class's public methods exist on both sides with the same parameters.
///
/// The dunders PyO3 generates from a `#[pyclass(eq)]` (and the like) are not
/// the stub's to mirror, so only public methods and `__init__` are compared.
fn method(inputs: &Inputs<'_>, reporter: &mut Reporter<'_>) {
    for (surface, binding) in inputs.binding_entries(Kind::Class) {
        let Some(stub) = inputs.stub_class(&binding.path) else {
            continue;
        };
        for declared in binding.methods.iter().filter(|method| method.is_public()) {
            match stub
                .methods
                .iter()
                .find(|found| found.name == declared.name)
            {
                None => reporter.report_position(
                    &surface.file,
                    format!("{}.{}", binding.path, declared.name),
                    declared.line,
                    declared.column,
                    format!("the stub declares no `{}`", declared.name),
                ),
                Some(declared_stub) if !same_params(&declared.params, &declared_stub.params) => {
                    reporter.report_position(
                        &inputs.stub.file,
                        format!("{}.{}", stub.path, declared_stub.name),
                        declared_stub.line,
                        declared_stub.column,
                        format!(
                            "the bindings take ({}), the stub takes ({})",
                            render_params(&declared.params),
                            render_params(&declared_stub.params)
                        ),
                    )
                }
                Some(_) => {}
            }
        }
        for declared in stub.methods.iter().filter(|method| method.is_public()) {
            if binding
                .methods
                .iter()
                .any(|found| found.name == declared.name)
            {
                continue;
            }
            reporter.report_position(
                &inputs.stub.file,
                format!("{}.{}", stub.path, declared.name),
                declared.line,
                declared.column,
                format!("the bindings expose no `{}`", declared.name),
            );
        }
    }
}

/// A record class is a `Record`: a class the bindings return through `attach`
/// is declared `(Record)` in the stub, and a stub class declared `(Record)` is
/// one the bindings return.
fn base(inputs: &Inputs<'_>, reporter: &mut Reporter<'_>) {
    let returned: BTreeSet<String> = inputs
        .binding_entries(Kind::Function)
        .filter_map(|(_, binding)| binding.returned_class())
        .collect();
    for name in &returned {
        let Some(stub) = inputs.stub_class_named(name) else {
            continue;
        };
        if stub.bases.iter().any(|base| base == "Record") {
            continue;
        }
        reporter.report(
            &inputs.stub.file,
            stub,
            format!("the bindings return `{name}`, but the stub does not declare it a `Record`"),
        );
    }
    for stub in &inputs.stub.entries {
        if !is_record(stub) || returned.contains(stub.name()) {
            continue;
        }
        reporter.report(
            &inputs.stub.file,
            stub,
            format!(
                "`{}` is declared a `Record`, but no command returns it",
                stub.name()
            ),
        );
    }
}

/// A record's fields match a model of the same name, and a field the model
/// skips when empty (`skip_serializing_if = "Option::is_none"`) is `| None`.
///
/// The name can be declared twice (the wire `v1::TableFile` and the streaming
/// `table::TableFile`), so the stub has to match one model, not all.
fn field(inputs: &Inputs<'_>, reporter: &mut Reporter<'_>) {
    for stub in record_classes(inputs) {
        let name = stub.name().to_owned();
        let Some(candidate) = matched_candidate(inputs, &name, &declared_names(stub)) else {
            let expected = inputs
                .model
                .candidates(&name)
                .iter()
                .map(|candidate| render_fields(candidate))
                .collect::<Vec<_>>()
                .join(" or ");
            reporter.report(
                &inputs.stub.file,
                stub,
                format!(
                    "`{name}` declares ({}) but the model declares {expected}",
                    render_fields(&declared_fields(stub))
                ),
            );
            continue;
        };
        for declared in &stub.fields {
            let Some(model) = candidate.iter().find(|model| model.name == declared.name) else {
                continue;
            };
            if declared.optional == model.optional {
                continue;
            }
            reporter.report_position(
                &inputs.stub.file,
                format!("{name}.{}", declared.name),
                declared.line,
                declared.column,
                optionality_message(declared, model),
            );
        }
    }
}

impl Inputs<'_> {
    /// The stub function at a path.
    fn function(&self, path: &str) -> Option<&Entry> {
        self.stub
            .get(path)
            .filter(|entry| entry.kind == Kind::Function)
    }
}

/// Why a field's optionality disagrees: the model skips it when empty and the
/// stub does not say so, or the other way round.
fn optionality_message(field: &Field, model: &ModelField) -> String {
    if model.optional {
        format!(
            "the model leaves `{}` out when it is empty; the stub should declare `{} | None`",
            field.name, field.name
        )
    } else {
        format!(
            "the model always writes `{}`; the stub should not make it `| None`",
            field.name
        )
    }
}

/// The stub classes that mirror a library model: the classes declared
/// `(Record)` that the library declares.
fn record_classes<'a>(inputs: &'a Inputs<'_>) -> impl Iterator<Item = &'a Entry> {
    inputs
        .stub
        .entries
        .iter()
        .filter(|entry| is_record(entry) && inputs.model.contains(entry.name()))
}

/// A class's declared field names, for the model comparison.
fn declared_names(entry: &Entry) -> BTreeSet<String> {
    entry
        .fields
        .iter()
        .map(|field| field.name.clone())
        .collect()
}

/// A class's declared fields in the model's shape, for a message.
fn declared_fields(entry: &Entry) -> Vec<ModelField> {
    entry
        .fields
        .iter()
        .map(|field| ModelField {
            name: field.name.clone(),
            optional: field.optional,
        })
        .collect()
}

/// The model candidate whose field names are exactly `declared`, if any.
fn matched_candidate<'a>(
    inputs: &'a Inputs<'_>,
    name: &str,
    declared: &BTreeSet<String>,
) -> Option<&'a Vec<ModelField>> {
    inputs.model.candidates(name).iter().find(|candidate| {
        candidate
            .iter()
            .map(|field| field.name.as_str())
            .collect::<BTreeSet<_>>()
            == declared.iter().map(String::as_str).collect()
    })
}

/// Whether a stub class is declared a `Record`.
fn is_record(entry: &Entry) -> bool {
    entry.kind == Kind::Class && entry.bases.iter().any(|base| base == "Record")
}

/// Whether a binding kind is a stub kind: a module is a nested-class proxy.
fn compatible(binding: Kind, stub: Kind) -> bool {
    binding == stub || matches!((binding, stub), (Kind::Module, Kind::Class))
}

/// Whether a path's last segment is a member of its parent's stub class.
fn member(stub: &Surface, path: &str) -> bool {
    let Some((parent, name)) = path.rsplit_once('.') else {
        return false;
    };
    let Some(owner) = stub.get(parent) else {
        return false;
    };
    owner.fields.iter().any(|field| field.name == name)
        || owner.methods.iter().any(|method| method.name == name)
}

fn same_params(left: &[Param], right: &[Param]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| left.name == right.name && left.has_default == right.has_default)
}

fn render_params(params: &[Param]) -> String {
    params
        .iter()
        .map(|param| {
            if param.has_default {
                format!("{}=", param.name)
            } else {
                param.name.clone()
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn render_fields(fields: &[ModelField]) -> String {
    fields
        .iter()
        .map(|field| field.name.clone())
        .collect::<Vec<_>>()
        .join(", ")
}

/// A return annotation with spacing removed and a trailing `| None` dropped, so
/// `Credentials | None` matches the `Credentials` an `Option` serializes to.
fn canonical(text: &str) -> String {
    let text = text.trim();
    let text = text
        .strip_suffix("| None")
        .map(str::trim_end)
        .unwrap_or(text);
    text.chars()
        .filter(|character| !character.is_whitespace())
        .collect()
}
