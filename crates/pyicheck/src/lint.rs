//! The check engine: options, findings, and the two surfaces a rule compares.

use std::path::{Path, PathBuf};

use crate::model::{Entry, ModelIndex, Surface};
use crate::rules::{self, Inputs};

/// How serious a finding is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    /// A stub that disagrees with the bindings it mirrors.
    ERROR,
    /// A finding from a heuristic that can be wrong in context.
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

/// One disagreement between the two surfaces, located in the stub.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Finding {
    /// Stable rule id, e.g. `surface/returns`.
    pub rule: &'static str,
    /// How serious the disagreement is.
    pub severity: Severity,
    /// The file the finding points at.
    pub file: PathBuf,
    /// 1-based line of the name.
    pub line: usize,
    /// 1-based byte column of the name.
    pub column: usize,
    /// The dotted Python name the finding is about.
    pub name: String,
    /// One-line explanation.
    pub message: String,
    /// Optional fix hint.
    pub help: Option<String>,
}

/// Options for a check run.
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

/// Check the stub against the bindings and the library models.
pub fn check(
    bindings: &[Surface],
    stub: &Surface,
    models: &ModelIndex,
    options: &Options,
) -> Vec<Finding> {
    let inputs = Inputs {
        bindings,
        stub,
        models,
    };
    let mut findings = Vec::new();
    for rule in rules::RULES {
        if options.enabled(rule.id) {
            (rule.check)(&inputs, &mut findings);
        }
    }
    findings.retain(|finding| options.keeps(finding.severity));
    findings.sort_by(|left, right| {
        (left.file.as_path(), left.line, left.column, left.rule).cmp(&(
            right.file.as_path(),
            right.line,
            right.column,
            right.rule,
        ))
    });
    findings
}

/// Build a finding; rules call this.
pub(crate) fn report(
    findings: &mut Vec<Finding>,
    rule: &'static str,
    severity: Severity,
    file: &Path,
    entry: &Entry,
    message: impl Into<String>,
    help: Option<String>,
) {
    report_position(
        findings,
        rule,
        severity,
        file,
        &entry.path,
        entry.line,
        entry.column,
        message,
        help,
    );
}

/// Build a finding at an explicit position, for a field or method inside a
/// class rather than the class name itself.
#[allow(clippy::too_many_arguments)]
pub(crate) fn report_position(
    findings: &mut Vec<Finding>,
    rule: &'static str,
    severity: Severity,
    file: &Path,
    name: impl Into<String>,
    line: usize,
    column: usize,
    message: impl Into<String>,
    help: Option<String>,
) {
    findings.push(Finding {
        rule,
        severity,
        file: file.to_path_buf(),
        line,
        column,
        name: name.into(),
        message: message.into(),
        help,
    });
}
