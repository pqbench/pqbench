//! Blackbox tests: parse a pair of snippets and check the findings through the
//! public API, the way the CLI calls it.

use pyicheck::lint::{check, Finding, Options};
use pyicheck::model::{ModelIndex, Surface};

/// A binding source exposing one verb, the shape every domain module uses.
const BINDING: &str = r#"
use crate::handles::Workspace;
use crate::prelude::*;
use crate::records::{one, Catalog};

/// The record class, as `record_class!` expands it.
#[pyclass]
pub(crate) struct Catalog;

#[pyfunction]
#[pyo3(name = "get")]
fn catalog_get(py: Python<'_>, workspace: PyRef<'_, Workspace>, name: String) -> PyResult<Py<PyAny>> {
    let workspace = workspace.0.clone();
    one(
        py,
        async move {
            pqbench::catalog::get::get(&workspace, &name)
                .await
                .map_err(|error| error.to_string())
        },
        Catalog::attach,
    )
}

pub(crate) fn register(py: Python<'_>, root: &Bound<'_, PyModule>) -> PyResult<()> {
    root.add_class::<Catalog>()?;
    let catalog = crate::submodule(py, root, "catalog")?;
    let get = crate::submodule(py, &catalog, "get")?;
    get.add_function(wrap_pyfunction!(catalog_get, &get)?)?;
    crate::expose(py, "pqbench.native.catalog", &catalog)?;
    crate::expose(py, "pqbench.native.catalog.get", &get)?;
    Ok(())
}
"#;

/// The stub that mirrors [`BINDING`].
const STUB: &str = r#"
class Catalog(Record):
    name: str

class catalog:
    class get:
        @staticmethod
        async def get(workspace: Workspace, name: str) -> Catalog: ...
"#;

fn surface(file: &str, entries: Vec<pyicheck::model::Entry>) -> Surface {
    Surface::new(file, entries)
}

fn binding(source: &str) -> Surface {
    surface("bindings.rs", pyicheck::rust::parse(source))
}

fn stub(source: &str) -> Surface {
    surface("native.pyi", pyicheck::python::parse(source))
}

fn models(source: &str) -> ModelIndex {
    let mut index = ModelIndex::default();
    pyicheck::library::collect(source, &mut index);
    index
}

fn run(binding_source: &str, stub_source: &str, library: &str) -> Vec<Finding> {
    check(
        &[binding(binding_source)],
        &stub(stub_source),
        &models(library),
        &Options::default(),
    )
}

/// The rules a run reports, in order.
fn rules(findings: &[Finding]) -> Vec<&str> {
    findings.iter().map(|finding| finding.rule).collect()
}

#[test]
fn a_matching_stub_is_clean() {
    assert!(run(BINDING, STUB, "").is_empty());
}

#[test]
fn a_name_the_stub_omits_is_reported() {
    let missing = STUB.replace("class catalog:\n    class get:\n        @staticmethod\n        async def get(workspace: Workspace, name: str) -> Catalog: ...\n", "");
    let findings = run(BINDING, &missing, "");
    assert!(
        rules(&findings).contains(&"surface/parity"),
        "an omitted verb is a parity finding: {:?}",
        rules(&findings)
    );
    assert!(
        findings
            .iter()
            .any(|finding| finding.name == "pqbench.native.catalog.get.get"),
        "the finding names the missing verb: {:?}",
        findings.iter().map(|f| &f.name).collect::<Vec<_>>()
    );
}

#[test]
fn a_return_the_stub_widens_is_reported() {
    let widened = STUB.replace("-> Catalog", "-> list[Catalog]");
    let findings = run(BINDING, &widened, "");
    assert_eq!(rules(&findings), ["surface/returns"]);
    assert!(
        findings[0].message.contains("list[Catalog]"),
        "the message names the disagreement: {}",
        findings[0].message
    );
}

#[test]
fn an_optional_return_matches_the_plain_class() {
    // `one(..., Credentials::attach)` wraps an `Option`, so the stub's `| None`
    // is the same shape, not a disagreement.
    let optional = STUB.replace("-> Catalog", "-> Catalog | None");
    assert!(run(BINDING, &optional, "").is_empty());
}

#[test]
fn a_parameter_the_stub_renames_is_reported() {
    let renamed = STUB.replace("name: str", "table: str");
    let findings = run(BINDING, &renamed, "");
    assert_eq!(rules(&findings), ["surface/params"]);
    assert!(
        findings[0].message.contains("(workspace, name)"),
        "the message names the binding parameters: {}",
        findings[0].message
    );
}

#[test]
fn a_default_the_stub_drops_is_reported() {
    let optional = BINDING.replace("name: String", "name: Option<String>");
    let binding = optional.replace(
        "#[pyo3(name = \"get\")]",
        "#[pyo3(name = \"get\")]\n#[pyo3(signature = (workspace, name=None))]",
    );
    // The stub keeps `name` required, so dropping the default is a finding.
    let findings = run(&binding, STUB, "");
    assert!(
        rules(&findings).contains(&"surface/params"),
        "a dropped default is a params finding: {:?}",
        rules(&findings)
    );
}

#[test]
fn an_awaitability_the_stub_loses_is_reported() {
    let sync = STUB.replace("async def get", "def get");
    let findings = run(BINDING, &sync, "");
    assert!(
        rules(&findings).contains(&"surface/async"),
        "a non-async stub for an awaitable call is an async finding: {:?}",
        rules(&findings)
    );
}

#[test]
fn a_record_field_the_model_lacks_is_reported() {
    let with_owner = STUB.replace("    name: str\n", "    name: str\n    owner: str\n");
    let library = r#"
pub struct Catalog {
    pub name: String,
    pub uid: Option<String>,
}
"#;
    let findings = run(BINDING, &with_owner, library);
    assert!(
        rules(&findings).contains(&"model/field"),
        "a stale field is a field finding: {:?}",
        rules(&findings)
    );
    assert!(
        findings
            .iter()
            .any(|finding| finding.rule == "model/field" && finding.message.contains("owner")),
        "the message names the stale field: {:?}",
        findings.iter().map(|f| &f.message).collect::<Vec<_>>()
    );
}

#[test]
fn a_record_field_set_that_matches_a_model_is_clean() {
    let library = r#"
pub struct Catalog {
    pub name: String,
    pub uid: Option<String>,
}
"#;
    // No `skip_serializing_if`, so serde always writes `uid`; the stub marks it
    // present, and both fields match.
    let stub_source = STUB.replace("    name: str\n", "    name: str\n    uid: str\n");
    assert!(run(BINDING, &stub_source, library).is_empty());
}

#[test]
fn a_model_the_stub_matches_one_of_is_clean() {
    // A record name can be declared twice (a wire model and a streaming one);
    // the stub has to match one, not all.
    let library = r#"
pub struct Catalog {
    pub name: String,
    pub credentials: Option<String>,
}
pub struct Catalog {
    pub name: String,
}
"#;
    assert!(run(BINDING, STUB, library).is_empty());
}

#[test]
fn a_class_the_bindings_do_not_register_is_reported() {
    let extra = format!("{STUB}\nclass Stray(Record):\n    name: str\n");
    let findings = run(BINDING, &extra, "");
    assert!(
        findings
            .iter()
            .any(|finding| finding.rule == "surface/parity"
                && finding.name == "pqbench.native.Stray"),
        "a class the bindings lack is a parity finding: {:?}",
        findings.iter().map(|f| &f.name).collect::<Vec<_>>()
    );
}

/// A class the bindings return must be declared a `Record` in the stub.
#[test]
fn a_returned_class_that_is_not_a_record_is_reported() {
    let findings = run(
        BINDING,
        &STUB.replace("class Catalog(Record):", "class Catalog:"),
        "",
    );
    assert!(
        findings.iter().any(|finding| finding.rule == "model/base"),
        "a returned class without `(Record)` is a base finding: {:?}",
        rules(&findings)
    );
}

/// A model field the stub makes optional but serde always writes.
#[test]
fn a_field_the_model_does_not_skip_is_reported_when_optional() {
    let library = r#"
pub struct Catalog {
    pub name: String,
    pub uid: Option<String>,
}
"#;
    // `uid` has no `skip_serializing_if`, so serde writes it; the stub must not
    // mark it `| None`.
    let stub_source = STUB.replace("    name: str\n", "    name: str\n    uid: str | None\n");
    let findings = run(BINDING, &stub_source, library);
    assert!(
        findings
            .iter()
            .any(|finding| finding.rule == "model/optionality"),
        "a wrongly-optional field is an optionality finding: {:?}",
        rules(&findings)
    );
}

/// A model field serde skips when empty, which the stub must mark `| None`.
#[test]
fn a_field_the_model_skips_must_be_optional() {
    let library = r#"
pub struct Catalog {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
}
"#;
    let stub_source = STUB.replace("    name: str\n", "    name: str\n    uid: str\n");
    let findings = run(BINDING, &stub_source, library);
    assert!(
        findings
            .iter()
            .any(|finding| finding.rule == "model/optionality"),
        "an unmarked absent-able field is an optionality finding: {:?}",
        rules(&findings)
    );
}

/// A class whose methods' signatures must mirror the bindings'.
const CLASS_BINDING: &str = r#"
use crate::prelude::*;

#[pyclass]
pub(crate) struct Config;

#[pymethods]
impl Config {
    #[new]
    #[pyo3(signature = (endpoint, token=None))]
    fn new(endpoint: String, token: Option<String>) -> Self {
        let _ = (endpoint, token);
        Config
    }

    #[getter]
    fn endpoint(&self) -> String {
        String::new()
    }
}

pub(crate) fn register(root: &Bound<'_, PyModule>) -> PyResult<()> {
    root.add_class::<Config>()?;
    Ok(())
}
"#;

const CLASS_STUB: &str = r#"
class Config:
    def __init__(self, endpoint: str, token: str | None = None) -> None: ...
    @property
    def endpoint(self) -> str: ...
"#;

#[test]
fn a_matching_class_is_clean() {
    assert!(run(CLASS_BINDING, CLASS_STUB, "").is_empty());
}

#[test]
fn a_dropped_method_is_reported() {
    let dropped = CLASS_STUB.replace("    @property\n    def endpoint(self) -> str: ...\n", "");
    let findings = run(CLASS_BINDING, &dropped, "");
    assert!(
        findings
            .iter()
            .any(|finding| finding.rule == "surface/method"),
        "a dropped method is a method finding: {:?}",
        rules(&findings)
    );
}

#[test]
fn a_constructor_default_the_stub_drops_is_reported() {
    let dropped = CLASS_STUB.replace("token: str | None = None", "token: str | None");
    let findings = run(CLASS_BINDING, &dropped, "");
    assert!(
        findings
            .iter()
            .any(|finding| finding.rule == "surface/method-params"),
        "a dropped `__init__` default is a method-params finding: {:?}",
        rules(&findings)
    );
}

/// The real wheel is clean: the rules run over the actual sources and find
/// nothing, which is what the CLI gate asserts.
#[test]
fn the_repository_is_clean() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let bindings_dir = root.join("python/src");
    let stub_path = root.join("python/pqbench/native.pyi");
    let library_dir = root.join("crates/pqbench/src");
    if !bindings_dir.is_dir() || !stub_path.is_file() || !library_dir.is_dir() {
        return;
    }

    let mut bindings = Vec::new();
    for path in sources(&bindings_dir, "rs") {
        let source = std::fs::read_to_string(&path).unwrap();
        let entries = pyicheck::rust::parse(&source);
        if !entries.is_empty() {
            bindings.push(Surface::new(path, entries));
        }
    }
    let stub_source = std::fs::read_to_string(&stub_path).unwrap();
    let stub = surface(
        "python/pqbench/native.pyi",
        pyicheck::python::parse(&stub_source),
    );
    let mut index = ModelIndex::default();
    for path in sources(&library_dir, "rs") {
        let source = std::fs::read_to_string(&path).unwrap();
        pyicheck::library::collect(&source, &mut index);
    }

    let findings = check(&bindings, &stub, &index, &Options::default());
    let rendered: Vec<String> = findings
        .iter()
        .map(|finding| format!("{}: {}", finding.name, finding.message))
        .collect();
    assert!(
        findings.is_empty(),
        "the wheel is consistent: {rendered:#?}"
    );
}

/// Every file of an extension under a directory.
fn sources(root: &std::path::Path, extension: &str) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|found| found == extension) {
                out.push(path);
            }
        }
    }
    out.sort();
    out
}
