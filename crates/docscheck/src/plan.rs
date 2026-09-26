//! Turn runnable blocks into steps, and a plan of many blocks.
//!
//! A block body is a shell script, but each command is split at top-level
//! newlines, honoring backslash continuations and pipes onto a following line,
//! so a generated test can attribute a failure to a `file:line` and run each
//! command in its own process. A leading `#` comment line is either a
//! [`Directive`] or is dropped; other comment lines stay with the command below.

use crate::model::{Block, Directive};

/// One command to run and the context it runs in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Step {
    /// The command text, continuations joined, no trailing newline.
    pub command: String,
    /// 1-based line in the source document where the command starts.
    pub line: usize,
    /// Working directory relative to the repository root, or `None` for the root.
    pub directory: Option<String>,
    /// Environment variables from directives, in order.
    pub variables: Vec<(String, String)>,
}

/// The steps of a runnable block, in order.
///
/// Returns an empty vector when the block is not marked `run`.
pub fn block_steps(block: &Block) -> Vec<Step> {
    if !block.is_runnable() {
        return Vec::new();
    }
    split_steps(block)
}

/// A whole-document plan: the runnable steps and the skipped blocks.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Plan {
    /// The steps to run, in document order.
    pub steps: Vec<Step>,
    /// Blocks that were skipped because they were not marked `run`.
    pub skipped: Vec<SkippedBlock>,
}

/// A block that carried no `run` marker and so was not executed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkippedBlock {
    /// The fence language, when present.
    pub language: Option<String>,
    /// 1-based line of the opening fence.
    pub line: usize,
}

impl Plan {
    /// Build a plan from one document's blocks.
    pub fn from_blocks(blocks: &[Block]) -> Self {
        let mut plan = Plan::default();
        for block in blocks {
            if !block.is_runnable() {
                plan.skipped.push(SkippedBlock {
                    language: block.language().map(str::to_owned),
                    line: block.line,
                });
                continue;
            }
            plan.steps.extend(split_steps(block));
        }
        plan
    }
}

/// Split a runnable block into steps, applying its directives.
fn split_steps(block: &Block) -> Vec<Step> {
    let mut directory = None;
    let mut variables = Vec::new();
    let mut steps = Vec::new();
    let mut pending: Option<(String, usize)> = None;

    for (offset, raw_line) in block.body.lines().enumerate() {
        let line_number = block.body_line + offset;
        let trimmed = raw_line.trim();

        if let Some(directive) = Directive::parse(raw_line) {
            apply(&mut directory, &mut variables, directive);
            continue;
        }
        if trimmed.is_empty() {
            continue;
        }
        if trimmed.starts_with('#') && pending.is_none() {
            continue;
        }

        match &mut pending {
            Some((command, _)) => {
                // Continuations keep their leading indentation, so a joined
                // command matches what the reader sees.
                command.push('\n');
                command.push_str(raw_line.trim_end());
                if !continues(trimmed) {
                    let (command, line) = pending.take().expect("pending command");
                    steps.push(Step {
                        command,
                        line,
                        directory: directory.clone(),
                        variables: variables.clone(),
                    });
                }
            }
            None => {
                let line = line_number;
                if continues(trimmed) {
                    pending = Some((trimmed.to_owned(), line));
                } else {
                    steps.push(Step {
                        command: trimmed.to_owned(),
                        line,
                        directory: directory.clone(),
                        variables: variables.clone(),
                    });
                }
            }
        }
    }

    if let Some((command, line)) = pending {
        steps.push(Step {
            command,
            line,
            directory,
            variables,
        });
    }
    steps
}

/// A line that ends in `\` or a pipe continues onto the next line.
fn continues(line: &str) -> bool {
    line.ends_with('\\') || line.ends_with('|')
}

fn apply(
    directory: &mut Option<String>,
    variables: &mut Vec<(String, String)>,
    directive: Directive,
) {
    match directive {
        Directive::Directory(path) => *directory = Some(path),
        Directive::Environment { name, value } => variables.push((name, value)),
    }
}
