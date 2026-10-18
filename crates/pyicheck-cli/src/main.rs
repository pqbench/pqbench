//! `pyicheck` — check the PyO3 stub against the bindings it mirrors.
//!
//! The default inputs are the wheel's own layout: the bindings in
//! `python/src`, the stub at `python/pqbench/native.pyi`, and the library's
//! models under `crates/pqbench/src`. Output is one line per finding by
//! default; `--output-format json` emits one object per finding and `github`
//! emits workflow commands annotated inline on a pull request. Exit status is 0
//! when clean, 1 when findings exist, and 2 for usage or io errors.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, ValueEnum};

use pyicheck::lint::{check, rule_ids, Finding, Options, Severity};
use pyicheck::model::{ModelIndex, Surface};

#[derive(Parser)]
#[command(
    name = "pyicheck",
    about = "Check the PyO3 stub against the bindings",
    after_help = r#"
The stub (python/pqbench/native.pyi) is hand-written, so it can drift from the
bindings (python/src) and the library models (crates/pqbench/src) it mirrors.
Findings print as file:line:column: severity[rule]: message.

Output formats:
  text    file:line:column: severity[rule]: message (default)
  json    one JSON object per finding, for jq and friends
  github  ::error/::warning workflow commands, annotated inline in a PR

Examples:
  pyicheck
  pyicheck --rule surface/returns
  pyicheck --output-format github
  pyicheck --output-format json | jq -r 'select(.severity == "error") | .rule'
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
    /// How to render findings.
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    output_format: OutputFormat,
    /// Shorthand for `--output-format json`.
    #[arg(long, hide = true)]
    json: bool,
    /// Restrict the run to a rule id (repeatable).
    #[arg(long = "rule", value_name = "ID")]
    rules: Vec<String>,
    /// Silence a rule id (repeatable).
    #[arg(long = "allow", value_name = "ID")]
    allowed_rules: Vec<String>,
    /// Drop findings below this severity.
    #[arg(long, value_enum)]
    min_severity: Option<SeverityArg>,
    /// Print a per-rule count after the findings.
    #[arg(long = "statistics")]
    stats: bool,
    /// Always exit 0, even when findings exist.
    #[arg(long)]
    exit_zero: bool,
    /// List the rule ids and exit.
    #[arg(long = "list-rules")]
    list_rules: bool,
}

#[derive(Clone, Copy, PartialEq, Eq, ValueEnum)]
enum OutputFormat {
    Text,
    Json,
    Github,
}

impl OutputFormat {
    fn resolve(self, json: bool) -> Self {
        if json {
            OutputFormat::Json
        } else {
            self
        }
    }

    fn render(self, finding: &Finding) -> String {
        match self {
            OutputFormat::Text => render_text(finding),
            OutputFormat::Json => render_json(finding),
            OutputFormat::Github => render_github(finding),
        }
    }
}

#[derive(Clone, Copy, clap::ValueEnum)]
enum SeverityArg {
    Error,
    Warning,
}

impl SeverityArg {
    fn severity(self) -> Severity {
        match self {
            SeverityArg::Error => Severity::ERROR,
            SeverityArg::Warning => Severity::WARNING,
        }
    }
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    if cli.list_rules {
        for id in rule_ids() {
            println!("{id}");
        }
        return ExitCode::SUCCESS;
    }

    let bindings = match read_bindings(&cli.binding_root) {
        Ok(bindings) => bindings,
        Err(error) => {
            eprintln!("pyicheck: {}: {error}", cli.binding_root.display());
            return ExitCode::from(2);
        }
    };
    let stub = match read_stub(&cli.stub) {
        Ok(stub) => stub,
        Err(error) => {
            eprintln!("pyicheck: {}: {error}", cli.stub.display());
            return ExitCode::from(2);
        }
    };
    let mut models = ModelIndex::default();
    if let Err(error) = read_models(&cli.library, &mut models) {
        eprintln!("pyicheck: {}: {error}", cli.library.display());
        return ExitCode::from(2);
    }

    let options = Options {
        rules: cli.rules,
        allowed_rules: cli.allowed_rules,
        min_severity: cli.min_severity.map(SeverityArg::severity),
    };
    let findings = check(&bindings, &stub, &models, &options);

    let format = cli.output_format.resolve(cli.json);
    for finding in &findings {
        println!("{}", format.render(finding));
    }
    if cli.stats {
        print_stats(&findings);
    }

    if findings.is_empty() || cli.exit_zero {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    }
}

/// Parse every binding source into one surface per file.
fn read_bindings(root: &Path) -> io::Result<Vec<Surface>> {
    let mut files = Vec::new();
    collect_extension(root, "rs", &mut files)?;
    files.sort();
    let mut surfaces = Vec::new();
    for path in files {
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
    let mut files = Vec::new();
    collect_extension(root, "rs", &mut files)?;
    files.sort();
    for path in files {
        let source = fs::read_to_string(&path)?;
        pyicheck::library::collect(&source, models);
    }
    Ok(())
}

/// Every file of an extension under a directory, `target` and hidden entries
/// skipped. A single file is taken as itself.
fn collect_extension(root: &Path, extension: &str, out: &mut Vec<PathBuf>) -> io::Result<()> {
    let metadata = fs::metadata(root)?;
    if !metadata.is_dir() {
        if root.extension().is_some_and(|found| found == extension) {
            out.push(root.to_path_buf());
        }
        return Ok(());
    }
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with('.') {
            continue;
        }
        let path = entry.path();
        if path.is_dir() {
            if name != "target" {
                collect_extension(&path, extension, out)?;
            }
        } else if path.extension().is_some_and(|found| found == extension) {
            out.push(path);
        }
    }
    Ok(())
}

fn render_text(finding: &Finding) -> String {
    let help = finding
        .help
        .as_deref()
        .map(|help| format!(" help: {help}"))
        .unwrap_or_default();
    format!(
        "{}:{}:{}: {}[{}]: {}{help}",
        finding.file.display(),
        finding.line,
        finding.column,
        finding.severity.as_str(),
        finding.rule,
        finding.message,
    )
}

fn render_json(finding: &Finding) -> String {
    serde_json::json!({
        "path": finding.file.display().to_string(),
        "line": finding.line,
        "column": finding.column,
        "severity": finding.severity.as_str(),
        "rule": finding.rule,
        "name": finding.name,
        "message": finding.message,
        "help": finding.help,
    })
    .to_string()
}

/// A GitHub Actions workflow command, rendered inline on the PR diff.
fn render_github(finding: &Finding) -> String {
    let level = match finding.severity {
        Severity::ERROR => "error",
        Severity::WARNING => "warning",
    };
    let help = finding
        .help
        .as_deref()
        .map(|help| format!(" (help: {help})"))
        .unwrap_or_default();
    let message = escape_github(&format!("{}{help}", finding.message));
    format!(
        "::{level} file={},line={},col={},title=pyicheck {rule}::{message}",
        finding.file.display(),
        finding.line,
        finding.column,
        rule = finding.rule,
    )
}

fn escape_github(text: &str) -> String {
    text.replace('%', "%25")
        .replace('\r', "%0D")
        .replace('\n', "%0A")
}

fn print_stats(findings: &[Finding]) {
    let mut counts: std::collections::BTreeMap<&str, usize> = std::collections::BTreeMap::new();
    for finding in findings {
        *counts.entry(finding.rule).or_default() += 1;
    }
    if counts.is_empty() {
        return;
    }
    eprintln!("pyicheck: {} finding(s)", findings.len());
    for (rule, count) in counts {
        eprintln!("{count:>4}  {rule}");
    }
}
