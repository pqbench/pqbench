//! The rule set: checks that compare the two surfaces, and the models behind
//! them.
//!
//! The stub is the hand-maintained mirror, so a finding points at the stub —
//! the fix goes there — and names what the bindings say. The one exception is a
//! name the stub declares and the bindings do not, which points at the stub
//! too: the stub is the artifact a reader trusts.

use std::collections::BTreeSet;

use crate::lint::{report, report_position, Finding, Severity};
use crate::model::{Entry, Field, Kind, ModelField, Param, Surface};

/// What a rule sees: both surfaces and the library models.
pub(crate) struct Inputs<'a> {
    /// The bindings, one surface per source file.
    pub bindings: &'a [Surface],
    /// The stub.
    pub stub: &'a Surface,
    /// The library's `pub struct` models, for the field rules.
    pub models: &'a crate::model::ModelIndex,
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

/// A single rule: a stable id and a check that pushes findings.
pub(crate) struct Rule {
    pub id: &'static str,
    pub check: fn(&Inputs<'_>, &mut Vec<Finding>),
}

/// Every rule, in report order.
pub(crate) const RULES: &[Rule] = &[
    Rule {
        id: "surface/parity",
        check: parity,
    },
    Rule {
        id: "surface/params",
        check: params,
    },
    Rule {
        id: "surface/returns",
        check: returns,
    },
    Rule {
        id: "surface/async",
        check: asynchronous,
    },
    Rule {
        id: "surface/method",
        check: methods,
    },
    Rule {
        id: "surface/method-params",
        check: method_params,
    },
    Rule {
        id: "model/base",
        check: bases,
    },
    Rule {
        id: "model/field",
        check: fields,
    },
    Rule {
        id: "model/optionality",
        check: optionality,
    },
];

/// Every name on one side exists on the other.
///
/// The stub writes a module as a nested class and may hang an export on that
/// class as an annotation instead of a nested class (`ratelimit.RateLimit`), so
/// a binding module is satisfied by a stub class of the same path, and a
/// binding name by a member of its parent's proxy class.
fn parity(inputs: &Inputs<'_>, findings: &mut Vec<Finding>) {
    for surface in inputs.bindings {
        for binding in &surface.entries {
            check_binding(inputs, surface, binding, findings);
        }
    }
    for stub in &inputs.stub.entries {
        if inputs.binding(&stub.path).is_none() {
            report(
                findings,
                "surface/parity",
                Severity::ERROR,
                &inputs.stub.file,
                stub,
                format!("the bindings expose no `{}`", stub.path),
                None,
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
    findings: &mut Vec<Finding>,
) {
    match inputs.stub.get(&binding.path) {
        Some(stub) if compatible(binding.kind, stub.kind) => {}
        Some(stub) => report(
            findings,
            "surface/parity",
            Severity::ERROR,
            &inputs.stub.file,
            stub,
            format!(
                "the bindings expose {} as a {}, the stub as a {}",
                binding.path,
                binding.kind.as_str(),
                stub.kind.as_str()
            ),
            None,
        ),
        None if member(inputs.stub, &binding.path) => {}
        None => report(
            findings,
            "surface/parity",
            Severity::ERROR,
            &surface.file,
            binding,
            format!("the stub declares no `{}`", binding.path),
            None,
        ),
    }
}

/// A function's parameters, name and default for name and default.
fn params(inputs: &Inputs<'_>, findings: &mut Vec<Finding>) {
    for (_, binding) in inputs.binding_entries(Kind::Function) {
        let Some(stub) = inputs
            .stub
            .get(&binding.path)
            .filter(|entry| entry.kind == Kind::Function)
        else {
            continue;
        };
        if same_params(&binding.params, &stub.params) {
            continue;
        }
        report(
            findings,
            "surface/params",
            Severity::ERROR,
            &inputs.stub.file,
            stub,
            format!(
                "the bindings take ({}), the stub takes ({})",
                render_params(&binding.params),
                render_params(&stub.params)
            ),
            None,
        );
    }
}

/// A function's return shape, as the wrapper recovers it and the stub writes it.
fn returns(inputs: &Inputs<'_>, findings: &mut Vec<Finding>) {
    for (_, binding) in inputs.binding_entries(Kind::Function) {
        let Some(expected) = binding.returns.as_deref() else {
            continue;
        };
        let Some(stub) = inputs
            .stub
            .get(&binding.path)
            .filter(|entry| entry.kind == Kind::Function)
        else {
            continue;
        };
        let Some(declared) = stub.returns.as_deref() else {
            report(
                findings,
                "surface/returns",
                Severity::ERROR,
                &inputs.stub.file,
                stub,
                format!("the bindings return `{expected}`, the stub declares no return"),
                None,
            );
            continue;
        };
        if canonical(expected) != canonical(declared) {
            report(
                findings,
                "surface/returns",
                Severity::ERROR,
                &inputs.stub.file,
                stub,
                format!("the bindings return `{expected}`, the stub declares `{declared}`"),
                None,
            );
        }
    }
}

/// Whether a function is awaitable, which the stub says with `async def`.
fn asynchronous(inputs: &Inputs<'_>, findings: &mut Vec<Finding>) {
    for (_, binding) in inputs.binding_entries(Kind::Function) {
        let Some(expected) = binding.is_async else {
            continue;
        };
        let Some(stub) = inputs
            .stub
            .get(&binding.path)
            .filter(|entry| entry.kind == Kind::Function)
        else {
            continue;
        };
        if stub.is_async == Some(expected) {
            continue;
        }
        let (binding_word, stub_word) = if expected {
            ("awaitable", "not awaitable")
        } else {
            ("not awaitable", "awaitable")
        };
        report(
            findings,
            "surface/async",
            Severity::ERROR,
            &inputs.stub.file,
            stub,
            format!("the binding call is {binding_word}, the stub declares it {stub_word}"),
            None,
        );
    }
}

/// A class's public methods exist on both sides: a dropped method is drift, but
/// the dunders PyO3 generates from `#[pyclass(eq)]` (and the like) are not the
/// stub's to mirror, so only public methods and `__init__` are compared.
fn methods(inputs: &Inputs<'_>, findings: &mut Vec<Finding>) {
    for (surface, binding) in inputs.binding_entries(Kind::Class) {
        let Some(stub) = inputs.stub_class(&binding.path) else {
            continue;
        };
        methods_missing_from_stub(surface, binding, stub, findings);
        methods_missing_from_bindings(inputs, binding, stub, findings);
    }
}

/// The binding's public methods the stub does not declare, at their position in
/// the binding.
fn methods_missing_from_stub(
    surface: &Surface,
    binding: &Entry,
    stub: &Entry,
    findings: &mut Vec<Finding>,
) {
    for method in binding.methods.iter().filter(|method| method.is_public()) {
        if has_method(stub, &method.name) {
            continue;
        }
        report_position(
            findings,
            "surface/method",
            Severity::WARNING,
            &surface.file,
            format!("{}.{}", binding.path, method.name),
            method.line,
            method.column,
            format!("the stub declares no `{}`", method.name),
            None,
        );
    }
}

/// The stub's public methods the bindings do not expose, at their position in
/// the stub.
fn methods_missing_from_bindings(
    inputs: &Inputs<'_>,
    binding: &Entry,
    stub: &Entry,
    findings: &mut Vec<Finding>,
) {
    for method in stub.methods.iter().filter(|method| method.is_public()) {
        if binding
            .methods
            .iter()
            .any(|found| found.name == method.name)
        {
            continue;
        }
        report_position(
            findings,
            "surface/method",
            Severity::WARNING,
            &inputs.stub.file,
            format!("{}.{}", stub.path, method.name),
            method.line,
            method.column,
            format!("the bindings expose no `{}`", method.name),
            None,
        );
    }
}

/// A method's parameters, for a method both sides declare. `__init__` is the
/// one that matters most: it is built from `#[new]` and `#[pyo3(signature)]`.
fn method_params(inputs: &Inputs<'_>, findings: &mut Vec<Finding>) {
    for (_, binding) in inputs.binding_entries(Kind::Class) {
        let Some(stub) = inputs.stub_class(&binding.path) else {
            continue;
        };
        for method in &binding.methods {
            let Some(declared) = stub.methods.iter().find(|found| found.name == method.name) else {
                continue;
            };
            if same_params(&method.params, &declared.params) {
                continue;
            }
            report_position(
                findings,
                "surface/method-params",
                Severity::ERROR,
                &inputs.stub.file,
                format!("{}.{}", stub.path, declared.name),
                declared.line,
                declared.column,
                format!(
                    "the bindings take ({}), the stub takes ({})",
                    render_params(&method.params),
                    render_params(&declared.params)
                ),
                None,
            );
        }
    }
}

/// A record class is a `Record`: a class the bindings return through `attach`
/// is declared `(Record)` in the stub, and a stub class declared `(Record)` is
/// one the bindings return.
fn bases(inputs: &Inputs<'_>, findings: &mut Vec<Finding>) {
    let returned: BTreeSet<String> = inputs
        .binding_entries(Kind::Function)
        .filter_map(|(_, binding)| binding.returned_class())
        .collect();
    returned_are_records(inputs, &returned, findings);
    records_are_returned(inputs, &returned, findings);
}

/// Every class the bindings return is declared `(Record)` in the stub.
fn returned_are_records(
    inputs: &Inputs<'_>,
    returned: &BTreeSet<String>,
    findings: &mut Vec<Finding>,
) {
    for name in returned {
        let Some(stub) = inputs.stub_class_named(name) else {
            continue;
        };
        if stub.bases.iter().any(|base| base == "Record") {
            continue;
        }
        report(
            findings,
            "model/base",
            Severity::ERROR,
            &inputs.stub.file,
            stub,
            format!("the bindings return `{name}`, but the stub does not declare it a `Record`"),
            None,
        );
    }
}

/// Every stub class declared `(Record)` is one the bindings return.
fn records_are_returned(
    inputs: &Inputs<'_>,
    returned: &BTreeSet<String>,
    findings: &mut Vec<Finding>,
) {
    for stub in &inputs.stub.entries {
        if !is_record(stub) || returned.contains(stub.name()) {
            continue;
        }
        report(
            findings,
            "model/base",
            Severity::WARNING,
            &inputs.stub.file,
            stub,
            format!(
                "`{}` is declared a `Record`, but no command returns it",
                stub.name()
            ),
            None,
        );
    }
}

/// Whether a stub class is declared a `Record`.
fn is_record(entry: &Entry) -> bool {
    entry.kind == Kind::Class && entry.bases.iter().any(|base| base == "Record")
}

/// A record class's fields match a model of the same name in the library.
fn fields(inputs: &Inputs<'_>, findings: &mut Vec<Finding>) {
    for stub in record_classes(inputs) {
        let name = stub.name().to_owned();
        if matched_candidate(inputs, &name, &declared_names(stub)).is_some() {
            continue;
        }
        let expected = inputs
            .models
            .candidates(&name)
            .iter()
            .map(|candidate| render_fields(candidate))
            .collect::<Vec<_>>()
            .join(" or ");
        report(
            findings,
            "model/field",
            Severity::WARNING,
            &inputs.stub.file,
            stub,
            format!(
                "`{name}` declares ({}) but the model declares {expected}",
                render_fields(&declared_fields(stub))
            ),
            None,
        );
    }
}

/// A record field's optionality matches the model's: the stub marks a field the
/// model skips when empty (`skip_serializing_if = "Option::is_none"`) as
/// `| None`, and only that field.
fn optionality(inputs: &Inputs<'_>, findings: &mut Vec<Finding>) {
    for stub in record_classes(inputs) {
        let name = stub.name().to_owned();
        let Some(candidate) = matched_candidate(inputs, &name, &declared_names(stub)) else {
            continue;
        };
        for field in &stub.fields {
            let Some(model) = candidate.iter().find(|model| model.name == field.name) else {
                continue;
            };
            if field.optional == model.optional {
                continue;
            }
            report_position(
                findings,
                "model/optionality",
                Severity::WARNING,
                &inputs.stub.file,
                format!("{name}.{}", field.name),
                field.line,
                field.column,
                optionality_message(field, model),
                None,
            );
        }
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

/// The stub classes that mirror a library model: the classes declared `(Record)`
/// that the library declares.
fn record_classes<'a>(inputs: &'a Inputs<'_>) -> impl Iterator<Item = &'a Entry> {
    inputs
        .stub
        .entries
        .iter()
        .filter(|entry| is_record(entry) && inputs.models.contains(entry.name()))
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
    inputs.models.candidates(name).iter().find(|candidate| {
        candidate
            .iter()
            .map(|field| field.name.as_str())
            .collect::<BTreeSet<_>>()
            == declared.iter().map(String::as_str).collect()
    })
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
    owner.fields.iter().any(|field| field.name == name) || has_method(owner, name)
}

fn has_method(class: &Entry, name: &str) -> bool {
    class.methods.iter().any(|method| method.name == name)
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
