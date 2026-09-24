//! The lint engine: options, findings, suppression, and rule dispatch.

use crate::decl::Declaration;
use crate::rules::{self, Context};
use crate::rust::RustParser;

/// How serious a finding is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    /// A naming convention the AIP text makes mandatory.
    ERROR,
    /// An advisory finding from a heuristic that can be wrong in context.
    WARNING,
}

impl Severity {
    /// The lower-case spelling used in output.
    pub fn as_str(self) -> &'static str {
        match self {
            Severity::ERROR => "error",
            Severity::WARNING => "warning",
        }
    }

    fn rank(self) -> u8 {
        match self {
            Severity::ERROR => 0,
            Severity::WARNING => 1,
        }
    }
}

/// One rule violation, located at the offending identifier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Finding {
    /// Stable rule id, e.g. `aip-140/prepositions`.
    pub rule: &'static str,
    /// How serious the violation is.
    pub severity: Severity,
    /// The identifier the finding is about.
    pub name: String,
    /// One-line explanation.
    pub message: String,
    /// Optional fix hint.
    pub help: Option<String>,
    /// 1-based line of the identifier.
    pub line: usize,
    /// 1-based byte column of the identifier.
    pub column: usize,
}

/// Options for a lint run.
#[derive(Debug, Default, Clone)]
pub struct Options {
    /// Restrict the run to these rule ids; empty selects every rule.
    pub rules: Vec<String>,
    /// Silence these rule ids.
    pub allowed_rules: Vec<String>,
    /// Drop findings below this severity.
    pub min_severity: Option<Severity>,
}

impl Options {
    fn enabled(&self, rule: &str) -> bool {
        if self.allowed_rules.iter().any(|allowed| allowed == rule) {
            return false;
        }
        self.rules.is_empty() || self.rules.iter().any(|selected| selected == rule)
    }

    fn keeps(&self, severity: Severity) -> bool {
        self.min_severity
            .is_none_or(|min| severity.rank() <= min.rank())
    }
}

/// The rule ids this build knows about, in report order.
pub fn rule_ids() -> Vec<&'static str> {
    rules::RULES.iter().map(|rule| rule.id).collect()
}

/// Lint one source text and return findings in position order.
pub fn lint_source(source: &str, options: &Options) -> Vec<Finding> {
    Linter::new(options.clone()).lint(source)
}

/// A linter that reuses its parser, for one worker thread.
pub struct Linter {
    parser: RustParser,
    options: Options,
}

impl Linter {
    /// Build a linter with the given options.
    pub fn new(options: Options) -> Self {
        Self {
            parser: RustParser::new(),
            options,
        }
    }

    /// Lint one source text and return findings in position order.
    pub fn lint(&mut self, source: &str) -> Vec<Finding> {
        let decls = self.parser.parse(source);
        let context = Context { decls: &decls };
        let mut findings = Vec::new();
        for rule in rules::RULES {
            if self.options.enabled(rule.id) {
                (rule.check)(&context, &mut findings);
            }
        }
        findings.retain(|finding| {
            self.options.keeps(finding.severity) && !is_suppressed(source, finding)
        });
        findings.sort_by(|a, b| (a.line, a.column, a.rule).cmp(&(b.line, b.column, b.rule)));
        findings
    }
}

/// Whether an `// aipnaming: allow(...)` comment covers the finding's line.
///
/// The comment may sit on the finding's own line or on one of the three lines
/// above it, which is how a per-field suppression is written in practice.
fn is_suppressed(source: &str, finding: &Finding) -> bool {
    let start = finding.line.saturating_sub(4);
    source
        .lines()
        .skip(start)
        .take(4)
        .filter_map(allow_directive)
        .flatten()
        .any(|allowed| allowed == "all" || allowed == finding.rule)
}

fn allow_directive(line: &str) -> Option<Vec<&str>> {
    let comment = line.split_once("//")?.1.trim_start();
    let rest = comment.strip_prefix("aipnaming:")?.trim_start();
    let rest = rest.strip_prefix("allow")?;
    let list = rest.trim_start().strip_prefix('(')?.split_once(')')?.0;
    Some(
        list.split([',', ' '])
            .filter(|rule| !rule.is_empty())
            .collect(),
    )
}

/// Build a finding for a declaration; rule modules call this.
pub(crate) fn report(
    findings: &mut Vec<Finding>,
    rule: &'static str,
    severity: Severity,
    decl: &Declaration,
    message: impl Into<String>,
    help: Option<String>,
) {
    findings.push(Finding {
        rule,
        severity,
        name: decl.name.clone(),
        message: message.into(),
        help,
        line: decl.line,
        column: decl.column,
    });
}
