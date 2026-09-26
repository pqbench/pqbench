//! Turn runnable `console` blocks into steps, and a plan of many blocks.
//!
//! A runnable block is a `console` transcript: a `$ `-prefixed line is a
//! command, and the lines that follow, up to the next `$ ` or a blank line, are
//! that command's expected stdout. A `> `-prefixed line continues the command
//! (a shell line continuation), so long pipes stay readable.
//!
//! ```text
//! $ pqbench bytemass examples/quickstart.parquet --format table
//! column  type   codec         encodings                 bytes  values
//! ------  -----  ------------  ------------------------  -----  ------
//! id      INT64  UNCOMPRESSED  PLAIN,RLE,RLE_DICTIONARY    102       8
//! files: 1
//! ```
//!
//! Each command becomes a [`Step`] carrying its expected output, so a generated
//! test can run it and compare. The generated support matches line by line,
//! token by token; the support module documents the tolerance markers.

use crate::model::{Block, Directive};

/// One command to run and the context it runs in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Step {
    /// The command text, continuations joined, no trailing newline.
    pub command: String,
    /// 1-based line in the source document where the command starts.
    pub line: usize,
    /// The documented stdout, one entry per line. The support module compares
    /// it token by token, with `±` and dash-run markers as tolerance. Empty
    /// means the test only checks the exit status.
    pub expected: Vec<String>,
    /// Working directory relative to the repository root, or `None` for the root.
    pub directory: Option<String>,
    /// Environment variables from directives, in order.
    pub variables: Vec<(String, String)>,
}

impl Step {
    /// Whether the step documents its output.
    pub fn has_expected(&self) -> bool {
        !self.expected.is_empty()
    }
}

/// The steps of a runnable block, in order.
///
/// Returns an empty vector when the block is not marked `run` or is not a
/// `console` transcript.
pub fn block_steps(block: &Block) -> Vec<Step> {
    if !block.is_runnable() || block.language() != Some("console") {
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

/// Split a runnable transcript into steps, applying its directives.
fn split_steps(block: &Block) -> Vec<Step> {
    let mut directory = None;
    let mut variables = Vec::new();
    let mut steps: Vec<Step> = Vec::new();
    // The command being built: its text, source line, and expected output.
    let mut pending: Option<Pending> = None;

    for (offset, raw_line) in block.body.lines().enumerate() {
        let line_number = block.body_line + offset;

        if let Some(directive) = Directive::parse(raw_line) {
            apply(&mut directory, &mut variables, directive);
            continue;
        }

        if let Some(rest) = raw_line.strip_prefix("$ ") {
            finish(&mut pending, &mut steps, &directory, &variables);
            pending = Some(Pending {
                command: rest.to_owned(),
                line: line_number,
                expected: Vec::new(),
            });
            continue;
        }
        if let Some(rest) = raw_line.strip_prefix("> ") {
            if let Some(pending) = &mut pending {
                pending.command.push('\n');
                pending.command.push_str(rest);
            }
            continue;
        }

        // A `#` line is a note (or a directive, handled above), never output.
        if raw_line.trim_start().starts_with('#') {
            continue;
        }

        // Inside a transcript entry: a plain line is expected output.
        if let Some(pending) = &mut pending {
            pending.expected.push(raw_line.to_owned());
        }
    }

    finish(&mut pending, &mut steps, &directory, &variables);
    steps
}

/// A `$ ` command being built, with the output lines documented under it.
struct Pending {
    command: String,
    line: usize,
    expected: Vec<String>,
}

/// Push the pending command as a step, dropping trailing blank output lines.
fn finish(
    pending: &mut Option<Pending>,
    steps: &mut Vec<Step>,
    directory: &Option<String>,
    variables: &[(String, String)],
) {
    let Some(mut pending) = pending.take() else {
        return;
    };
    while pending
        .expected
        .last()
        .is_some_and(|line| line.trim().is_empty())
    {
        pending.expected.pop();
    }
    steps.push(Step {
        command: pending.command,
        line: pending.line,
        expected: pending.expected,
        directory: directory.clone(),
        variables: variables.to_vec(),
    });
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
