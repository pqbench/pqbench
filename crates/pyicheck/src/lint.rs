//! The check engine: findings, the reporter a rule writes through, and the two
//! surfaces a rule compares.

use std::path::{Path, PathBuf};

use crate::model::{Entry, ModelIndex, Surface};
use crate::rules::{self, Inputs};

/// How serious a finding is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    /// The stub disagrees with the bindings it mirrors.
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
}

/// One disagreement between the two surfaces.
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
}

/// The findings one rule is producing: its id and severity, so a rule says only
/// where and what.
pub(crate) struct Reporter<'a> {
    findings: &'a mut Vec<Finding>,
    rule: &'static str,
    severity: Severity,
}

impl Reporter<'_> {
    /// Report at a name's own position.
    pub(crate) fn report(&mut self, file: &Path, entry: &Entry, message: impl Into<String>) {
        self.push(file, entry.path.clone(), entry.line, entry.column, message);
    }

    /// Report at an explicit position, for a field or method inside a class.
    pub(crate) fn report_position(
        &mut self,
        file: &Path,
        name: impl Into<String>,
        line: usize,
        column: usize,
        message: impl Into<String>,
    ) {
        self.push(file, name.into(), line, column, message);
    }

    fn push(
        &mut self,
        file: &Path,
        name: String,
        line: usize,
        column: usize,
        message: impl Into<String>,
    ) {
        self.findings.push(Finding {
            rule: self.rule,
            severity: self.severity,
            file: file.to_path_buf(),
            line,
            column,
            name,
            message: message.into(),
        });
    }
}

/// Restrict a run to these rule ids; empty selects every rule.
pub type Options = Vec<String>;

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
        model: models,
    };
    let mut findings = Vec::new();
    for rule in rules::RULES {
        if !options.is_empty() && !options.iter().any(|id| id == rule.id) {
            continue;
        }
        let mut reporter = Reporter {
            findings: &mut findings,
            rule: rule.id,
            severity: rule.severity,
        };
        (rule.check)(&inputs, &mut reporter);
    }
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
