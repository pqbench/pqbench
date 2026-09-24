//! `aipnaming` — lint Rust names against the AIP naming conventions.
//!
//! Output is one line per finding by default. `--output-format json` emits one
//! object per finding and `--output-format github` emits workflow commands that
//! GitHub renders as inline annotations on a pull request. Exit status is 0
//! when clean, 1 when findings exist, and 2 for usage or io errors.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, ValueEnum};

use aipnaming::lint::{rule_ids, Finding, Options, Severity};

mod walk;

#[derive(Parser)]
#[command(
    name = "aipnaming",
    about = "Lint Rust names against the AIP naming conventions",
    after_help = r#"
Rules follow Google's AIPs (126/136/140/141/142/145/190), seeded from
api-linter. Findings print as file:line:column: severity[rule]: message.

Output formats:
  text    file:line:column: severity[rule]: message (default)
  json    one JSON object per finding, for jq and friends
  github  ::error/::warning workflow commands, annotated inline in a PR

Examples:
  aipnaming crates/pqbench/src
  aipnaming --rule aip-140/booleans --statistics crates/
  aipnaming --output-format github crates/
  aipnaming --output-format json . | jq -r 'select(.severity == "error") | .rule'
"#
)]
struct Cli {
    /// Files or directories to lint (default: the current directory).
    paths: Vec<PathBuf>,
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
    #[arg(long)]
    list_rules: bool,
}

#[derive(Clone, Copy, PartialEq, Eq, ValueEnum)]
enum OutputFormat {
    Text,
    Json,
    Github,
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
        allowed_rules: cli.allowed_rules,
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

    let format = if cli.json {
        OutputFormat::Json
    } else {
        cli.output_format
    };
    for (path, finding) in &report.findings {
        match format {
            OutputFormat::Text => println!("{}", render_text(path, finding)),
            OutputFormat::Json => println!("{}", render_json(path, finding)),
            OutputFormat::Github => println!("{}", render_github(path, finding)),
        }
    }
    if cli.stats {
        print_stats(&report.findings);
    }
    for (path, error) in &report.read_errors {
        eprintln!("aipnaming: cannot read {}: {error}", path.display());
    }

    if !report.read_errors.is_empty() {
        ExitCode::from(2)
    } else if report.findings.is_empty() || cli.exit_zero {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    }
}

fn render_text(path: &Path, finding: &Finding) -> String {
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

fn render_json(path: &Path, finding: &Finding) -> String {
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

/// A GitHub Actions workflow command, rendered inline on the PR diff.
///
/// `::error file=...,line=...,col=...::message` is the annotation protocol; the
/// rule id goes in `title` so the check name and the message stay readable.
fn render_github(path: &Path, finding: &Finding) -> String {
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
        "::{level} file={},line={},col={},title=aipnaming {rule}::{message}",
        path.display(),
        finding.line,
        finding.column,
        rule = finding.rule,
    )
}

/// Workflow-command data escapes `%`, `\r`, and `\n`; the message is a single
/// line, but `%` and newlines in help text must still be escaped.
fn escape_github(text: &str) -> String {
    text.replace('%', "%25")
        .replace('\r', "%0D")
        .replace('\n', "%0A")
}

fn print_stats(findings: &[(PathBuf, Finding)]) {
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for (_, finding) in findings {
        *counts.entry(finding.rule).or_default() += 1;
    }
    if counts.is_empty() {
        return;
    }
    eprintln!("aipnaming: {} finding(s)", findings.len());
    for (rule, count) in counts {
        eprintln!("{count:>4}  {rule}");
    }
}
