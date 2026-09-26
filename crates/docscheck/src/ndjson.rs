//! Require that a documented output is the human table, not NDJSON.
//!
//! A `console` transcript shows what a user sees: on a terminal `pqbench`
//! prints a table, and only a pipe (or `--format json`) emits NDJSON. A
//! transcript that documents NDJSON therefore teaches the wrong default, so
//! [`ndjson_outputs`] finds those steps and the caller fails the run. A block
//! is exempt when its fence carries the `json` word, which marks an example
//! that is *about* the machine output.

use crate::model::Block;
use crate::plan::{block_steps, Step};

/// The prefix of a `pqbench` NDJSON record. A documented output line that
/// starts with it is machine output, not a table.
const RECORD_PREFIX: &str = r#"{"kind":"pqbench"#;

/// Whether a documented output line is a `pqbench` NDJSON record.
pub fn is_ndjson(line: &str) -> bool {
    line.trim_start().starts_with(RECORD_PREFIX)
}

/// One command whose documented output is NDJSON without the `json` opt-in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NdjsonStep {
    /// 1-based line of the command in the source document.
    pub line: usize,
    /// The first offending output line, for the error message.
    pub output: String,
}

/// The steps that document NDJSON in a block not marked `json`.
///
/// Only runnable `console` blocks have steps, so prose JSON, a `no-run`
/// example, and a piped-away stage are not offenders.
pub fn ndjson_outputs(blocks: &[Block]) -> Vec<NdjsonStep> {
    let mut found = Vec::new();
    for block in blocks {
        if block.info.json {
            continue;
        }
        for step in block_steps(block) {
            if let Some(output) = first_record(&step) {
                found.push(NdjsonStep {
                    line: step.line,
                    output: output.clone(),
                });
            }
        }
    }
    found
}

/// The first documented NDJSON line of a step, if any.
fn first_record(step: &Step) -> Option<&String> {
    step.expected.iter().find(|line| is_ndjson(line))
}
