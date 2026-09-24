//! `aipnaming` — lint Rust names against the AIP naming conventions.
//!
//! Output is one line per finding, or one JSON object per finding with
//! `--json`, so the stream composes with the usual text tools. Exit status is
//! 0 when clean, 1 when findings exist, and 2 for usage or io errors.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;

use aipnaming::lint::{rule_ids, Options, Severity};

mod walk;

#[derive(Parser)]
#[command(
    name = "aipnaming",
    about = "Lint Rust names against the AIP naming conventions",
    after_help = r#"
Rules follow Google's AIPs (126/136/140/141/142/145/190), seeded from
api-linter. Findings print as file:line:column: severity[rule]: message.

Examples:
  aipnaming crates/pqbench/src
  aipnaming --rule aip-140/booleans --rule aip-140/prepositions src
  aipnaming --json . | jq -r 'select(.severity == "error") | .rule'
"#
)]
struct Cli {
    /// Files or directories to lint (default: the current directory).
    paths: Vec<PathBuf>,
    /// Emit one JSON object per finding.
    #[arg(long)]
    json: bool,
    /// Restrict the run to a rule id (repeatable).
    #[arg(long = "rule", value_name = "ID")]
    rules: Vec<String>,
    /// Silence a rule id (repeatable).
    #[arg(long = "allow", value_name = "ID")]
    allow: Vec<String>,
    /// Drop findings below this severity.
    #[arg(long, value_enum)]
    min_severity: Option<SeverityArg>,
    /// List the rule ids and exit.
    #[arg(long)]
    list_rules: bool,
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

    let options = Options {
        rules: cli.rules,
        allow: cli.allow,
        min_severity: cli.min_severity.map(SeverityArg::severity),
    };
    let files = match walk::rust_files(&cli.paths) {
        Ok(files) => files,
        Err(error) => {
            eprintln!("aipnaming: {error}");
            return ExitCode::from(2);
        }
    };
    let report = walk::lint_files(&files, options);

    for (path, finding) in &report.findings {
        if cli.json {
            println!("{}", render_json(path, finding));
        } else {
            println!("{}", render_text(path, finding));
        }
    }
    for (path, error) in &report.read_errors {
        eprintln!("aipnaming: cannot read {}: {error}", path.display());
    }

    if !report.read_errors.is_empty() {
        ExitCode::from(2)
    } else if report.findings.is_empty() {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    }
}

fn render_text(path: &std::path::Path, finding: &aipnaming::lint::Finding) -> String {
    let help = finding
        .help
        .as_deref()
        .map(|help| format!(" help: {help}"))
        .unwrap_or_default();
    format!(
        "{}:{}:{}: {}[{}]: {}{help}",
        path.display(),
        finding.line,
        finding.column,
        finding.severity.as_str(),
        finding.rule,
        finding.message,
    )
}

fn render_json(path: &std::path::Path, finding: &aipnaming::lint::Finding) -> String {
    serde_json::json!({
        "path": path.display().to_string(),
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
