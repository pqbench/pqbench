//! `pyicheck` — check the PyO3 stub against the bindings it mirrors.
//!
//! The default inputs are the wheel's own layout: the bindings in `python/src`,
//! the stub at `python/pqbench/native.pyi`, and the library's models under
//! `crates/pqbench/src`. Findings print one per line as
//! `file:line:column: severity[rule]: message`. Exit status is 0 when clean, 1
//! when findings exist, and 2 for usage or io errors.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::Parser;

use pyicheck::lint::{check, rule_ids, Finding};
use pyicheck::model::{ModelIndex, Surface};

#[derive(Parser)]
#[command(
    name = "pyicheck",
    about = "Check the PyO3 stub against the bindings",
    after_help = r#"
The stub (python/pqbench/native.pyi) is hand-written, so it can drift from the
bindings (python/src) and the library models (crates/pqbench/src) it mirrors.
Findings print as file:line:column: severity[rule]: message.

Examples:
  pyicheck
  pyicheck --rule surface/returns
  pyicheck --list-rules
"#
)]
struct Cli {
    /// The PyO3 binding sources.
    #[arg(long = "bindings", default_value = "python/src")]
    binding_root: PathBuf,
    /// The hand-written stub.
    #[arg(long, default_value = "python/pqbench/native.pyi")]
    stub: PathBuf,
    /// The library sources that declare the record models.
    #[arg(long, default_value = "crates/pqbench/src")]
    library: PathBuf,
    /// Restrict the run to a rule id (repeatable).
    #[arg(long = "rule", value_name = "ID")]
    rules: Vec<String>,
    /// Always exit 0, even when findings exist.
    #[arg(long)]
    exit_zero: bool,
    /// List the rule ids and exit.
    #[arg(long = "list-rules")]
    list_rules: bool,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    if cli.list_rules {
        for id in rule_ids() {
            println!("{id}");
        }
        return ExitCode::SUCCESS;
    }

    let inputs = match load(&cli) {
        Ok(inputs) => inputs,
        Err(error) => {
            eprintln!("pyicheck: {}: {}", error.path.display(), error.error);
            return ExitCode::from(2);
        }
    };
    let findings = check(&inputs.bindings, &inputs.stub, &inputs.model, &cli.rules);
    for finding in &findings {
        println!("{}", render(finding));
    }

    if findings.is_empty() || cli.exit_zero {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    }
}

/// The three inputs a run compares.
struct Inputs {
    bindings: Vec<Surface>,
    stub: Surface,
    model: ModelIndex,
}

/// An input that could not be read, with the path to name in the message.
struct LoadError {
    path: PathBuf,
    error: io::Error,
}

/// Read the bindings, the stub, and the library models.
fn load(cli: &Cli) -> Result<Inputs, LoadError> {
    let bindings = read_bindings(&cli.binding_root).map_err(|error| LoadError {
        path: cli.binding_root.clone(),
        error,
    })?;
    let stub = read_stub(&cli.stub).map_err(|error| LoadError {
        path: cli.stub.clone(),
        error,
    })?;
    let mut model = ModelIndex::default();
    read_models(&cli.library, &mut model).map_err(|error| LoadError {
        path: cli.library.clone(),
        error,
    })?;
    Ok(Inputs {
        bindings,
        stub,
        model,
    })
}

/// Parse every binding source into one surface per file.
fn read_bindings(root: &Path) -> io::Result<Vec<Surface>> {
    let mut surfaces = Vec::new();
    for path in sources(root)? {
        let source = fs::read_to_string(&path)?;
        let entries = pyicheck::rust::parse(&source);
        if !entries.is_empty() {
            surfaces.push(Surface::new(path, entries));
        }
    }
    Ok(surfaces)
}

/// Parse the stub.
fn read_stub(path: &Path) -> io::Result<Surface> {
    let source = fs::read_to_string(path)?;
    Ok(Surface::new(path, pyicheck::python::parse(&source)))
}

/// Read every library source into the model index.
fn read_models(root: &Path, models: &mut ModelIndex) -> io::Result<()> {
    for path in sources(root)? {
        let source = fs::read_to_string(&path)?;
        pyicheck::library::collect(&source, models);
    }
    Ok(())
}

/// Every `.rs` file under a directory, `target` and hidden entries skipped. A
/// single file is taken as itself.
fn sources(root: &Path) -> io::Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    if fs::metadata(root)?.is_dir() {
        collect_dir(root, &mut out)?;
    } else if root.extension().is_some_and(|found| found == "rs") {
        out.push(root.to_path_buf());
    }
    out.sort();
    Ok(out)
}

/// Every `.rs` file under a directory, one level down.
fn collect_dir(dir: &Path, out: &mut Vec<PathBuf>) -> io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with('.') {
            continue;
        }
        let path = entry.path();
        if path.is_dir() {
            if name != "target" {
                collect_dir(&path, out)?;
            }
        } else if path.extension().is_some_and(|found| found == "rs") {
            out.push(path);
        }
    }
    Ok(())
}

/// One finding, as `file:line:column: severity[rule]: message`.
fn render(finding: &Finding) -> String {
    format!(
        "{}:{}:{}: {}[{}]: {}",
        finding.file.display(),
        finding.line,
        finding.column,
        finding.severity.as_str(),
        finding.rule,
        finding.message,
    )
}
