//! Blackbox tests: parse a pair of snippets and check the findings through the
//! public API, the way the CLI calls it.

use pyicheck::lint::{check, Finding, Options};
use pyicheck::model::{ModelIndex, Surface};

/// A binding source exposing one verb, the shape every domain module uses.
const BINDING: &str = r#"
use crate::records::{one, Catalog};

#[pyclass]
pub(crate) struct Catalog;

#[pyfunction]
#[pyo3(name = "get")]
fn catalog_get(py: Python<'_>, workspace: PyRef<'_, Workspace>, name: String) -> PyResult<Py<PyAny>> {
    one(py, async move {
        pqbench::catalog::get::get(&workspace, &name).await.map_err(|e| e.to_string())
    }, Catalog::attach)
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

/// A class whose methods must mirror the bindings'.
const CLASS_BINDING: &str = r#"
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

/// A model whose `uid` is written always.
const LIB_PLAIN: &str = r#"
pub struct Catalog {
    pub name: String,
    pub uid: Option<String>,
}
"#;

/// A model whose `uid` serde skips when empty.
const LIB_SKIP: &str = r#"
pub struct Catalog {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
}
"#;

fn run(binding_source: &str, stub_source: &str, library: &str) -> Vec<Finding> {
    let mut models = ModelIndex::default();
    pyicheck::library::collect(library, &mut models);
    let bindings = [Surface::new(
        "bindings.rs",
        pyicheck::rust::parse(binding_source),
    )];
    let stub = Surface::new("native.pyi", pyicheck::python::parse(stub_source));
    check(&bindings, &stub, &models, &Options::default())
}

/// The rules a run reports, in order.
fn rules(findings: &[Finding]) -> Vec<&'static str> {
    findings.iter().map(|finding| finding.rule).collect()
}

/// Whether a run reports a rule.
fn has(binding: &str, stub: &str, library: &str, rule: &str) -> bool {
    rules(&run(binding, stub, library)).contains(&rule)
}

/// Whether a run reports a name.
fn names(binding: &str, stub: &str, name: &str) -> bool {
    run(binding, stub, "")
        .iter()
        .any(|finding| finding.name == name)
}

#[test]
fn a_matching_stub_is_clean() {
    assert!(run(BINDING, STUB, "").is_empty());
}

#[test]
fn a_matching_class_is_clean() {
    assert!(run(CLASS_BINDING, CLASS_STUB, "").is_empty());
}

#[test]
fn a_name_the_stub_omits_is_reported() {
    let verb = "class catalog:\n    class get:\n        @staticmethod\n        async def get(workspace: Workspace, name: str) -> Catalog: ...\n";
    assert!(names(
        BINDING,
        &STUB.replace(verb, ""),
        "pqbench.native.catalog.get.get"
    ));
}

#[test]
fn a_class_the_bindings_do_not_register_is_reported() {
    let extra = format!("{STUB}\nclass Stray(Record):\n    name: str\n");
    assert!(names(BINDING, &extra, "pqbench.native.Stray"));
}

#[test]
fn a_return_the_stub_widens_is_reported() {
    let findings = run(BINDING, &STUB.replace("-> Catalog", "-> list[Catalog]"), "");
    assert_eq!(rules(&findings), ["surface/returns"]);
    assert!(findings[0].message.contains("list[Catalog]"));
}

#[test]
fn an_optional_return_matches_the_plain_class() {
    // `one(..., Credentials::attach)` wraps an `Option`, so `| None` is the
    // same shape, not a disagreement.
    assert!(run(
        BINDING,
        &STUB.replace("-> Catalog", "-> Catalog | None"),
        ""
    )
    .is_empty());
}

#[test]
fn an_awaitability_the_stub_loses_is_reported() {
    assert!(has(
        BINDING,
        &STUB.replace("async def get", "def get"),
        "",
        "surface/returns"
    ));
}

#[test]
fn a_parameter_the_stub_renames_is_reported() {
    let findings = run(BINDING, &STUB.replace("name: str", "table: str"), "");
    assert_eq!(rules(&findings), ["surface/params"]);
    assert!(findings[0].message.contains("(workspace, name)"));
}

#[test]
fn a_default_the_stub_drops_is_reported() {
    let binding = BINDING.replace(
        "#[pyo3(name = \"get\")]",
        "#[pyo3(name = \"get\")]\n#[pyo3(signature = (workspace, name=None))]",
    );
    assert!(has(&binding, STUB, "", "surface/params"));
}

#[test]
fn a_dropped_method_is_reported() {
    let dropped = CLASS_STUB.replace("    @property\n    def endpoint(self) -> str: ...\n", "");
    assert!(has(CLASS_BINDING, &dropped, "", "surface/method"));
}

#[test]
fn a_constructor_default_the_stub_drops_is_reported() {
    let dropped = CLASS_STUB.replace("token: str | None = None", "token: str | None");
    assert!(has(CLASS_BINDING, &dropped, "", "surface/method"));
}

#[test]
fn a_returned_class_that_is_not_a_record_is_reported() {
    assert!(has(
        BINDING,
        &STUB.replace("class Catalog(Record):", "class Catalog:"),
        "",
        "model/base"
    ));
}

#[test]
fn a_record_field_the_model_lacks_is_reported() {
    let stub = STUB.replace("    name: str\n", "    name: str\n    owner: str\n");
    let findings = run(BINDING, &stub, LIB_PLAIN);
    assert!(findings
        .iter()
        .any(|finding| finding.message.contains("owner")));
}

#[test]
fn a_record_field_set_that_matches_a_model_is_clean() {
    let stub = STUB.replace("    name: str\n", "    name: str\n    uid: str\n");
    assert!(run(BINDING, &stub, LIB_PLAIN).is_empty());
}

#[test]
fn a_field_the_model_does_not_skip_is_reported_when_optional() {
    let stub = STUB.replace("    name: str\n", "    name: str\n    uid: str | None\n");
    assert!(has(BINDING, &stub, LIB_PLAIN, "model/field"));
}

#[test]
fn a_field_the_model_skips_must_be_optional() {
    let stub = STUB.replace("    name: str\n", "    name: str\n    uid: str\n");
    assert!(has(BINDING, &stub, LIB_SKIP, "model/field"));
}

#[test]
fn a_model_the_stub_matches_one_of_is_clean() {
    // A record name can be declared twice (a wire model and a streaming one);
    // the stub has to match one, not all.
    let library = format!("{LIB_PLAIN}\npub struct Catalog {{\n    pub name: String,\n}}");
    assert!(run(BINDING, STUB, &library).is_empty());
}

/// The real wheel is clean: the rules run over the actual sources and find
/// nothing, which is what the CLI gate asserts.
#[test]
fn the_repository_is_clean() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let stub_path = root.join("python/pqbench/native.pyi");
    if !root.join("python/src").is_dir() || !stub_path.is_file() {
        return;
    }
    let mut bindings = Vec::new();
    for path in sources(&root.join("python/src")) {
        let entries = pyicheck::rust::parse(&std::fs::read_to_string(&path).unwrap());
        if !entries.is_empty() {
            bindings.push(Surface::new(path, entries));
        }
    }
    let mut models = ModelIndex::default();
    for path in sources(&root.join("crates/pqbench/src")) {
        pyicheck::library::collect(&std::fs::read_to_string(&path).unwrap(), &mut models);
    }
    let stub = Surface::new(
        "native.pyi",
        pyicheck::python::parse(&std::fs::read_to_string(&stub_path).unwrap()),
    );
    let findings = check(&bindings, &stub, &models, &Options::default());
    let rendered: Vec<String> = findings
        .iter()
        .map(|finding| format!("{}: {}", finding.name, finding.message))
        .collect();
    assert!(
        findings.is_empty(),
        "the wheel is consistent: {rendered:#?}"
    );
}

/// Every `.rs` file under a directory.
fn sources(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|found| found == "rs") {
                out.push(path);
            }
        }
    }
    out.sort();
    out
}
