//! Require that a `pqbench` command example is a `console` block.
//!
//! A command example only becomes a test when it lives in a `console` fence, so
//! a `pqbench` example written as `sh` (or a bare fence) would silently go
//! untested. [`pqbench_outside_console`] finds those blocks so the caller can
//! fail the run.

use crate::model::Block;

/// Shell tokens after which the next token starts a command: a pipeline, a
/// list, or `xargs`.
const COMMAND_SEPARATORS: &[&str] = &["|", "||", "&&", ";", "xargs"];

/// Whether a block body invokes the `pqbench` binary as a command.
///
/// The `pqbench` token must sit in command position — at the start of a line
/// (after optional `NAME=value` assignments), or after a shell separator or
/// `xargs`. That keeps prose and data that merely mention the name out: a JSON
/// `"kind":"pqbench.bytemass"` field, `import pqbench`, and
/// `pqbench/pqbench:latest` are not commands.
pub fn invokes_pqbench(body: &str) -> bool {
    body.lines().any(line_invokes_pqbench)
}

fn line_invokes_pqbench(line: &str) -> bool {
    let line = line.trim_start();
    // A `#` line is a note, not a command.
    if line.starts_with('#') {
        return false;
    }
    let mut command_position = true;
    let mut previous = "";
    for token in line.split_whitespace() {
        if token == "pqbench" && (command_position || COMMAND_SEPARATORS.contains(&previous)) {
            return true;
        }
        if command_position && !is_assignment(token) {
            command_position = false;
        }
        previous = token;
    }
    false
}

/// Whether a token is a leading `NAME=value` environment assignment.
fn is_assignment(token: &str) -> bool {
    let Some((name, _)) = token.split_once('=') else {
        return false;
    };
    !name.is_empty()
        && name
            .chars()
            .next()
            .is_some_and(|ch| ch.is_ascii_alphabetic() || ch == '_')
        && name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

/// Blocks that invoke `pqbench` but whose fence is not `console`.
pub fn pqbench_outside_console(blocks: &[Block]) -> Vec<&Block> {
    blocks
        .iter()
        .filter(|block| block.language() != Some("console") && invokes_pqbench(&block.body))
        .collect()
}
