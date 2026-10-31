//! Generate the CLI reference from the pqbench clap tree.
//!
//! `docsgen` is a development tool: it depends on the CLI library for the
//! command definitions, but it is not part of the shipped binary. `write`
//! regenerates the committed page, `check` fails when it is stale, and `print`
//! writes it to stdout.

use std::path::{Path, PathBuf};

use clap::CommandFactory;
use clap_markdown::MarkdownOptions;

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut args = std::env::args().skip(1);
    let mode = args.next().unwrap_or_else(|| "print".to_string());
    let path = args.next().map(PathBuf::from).unwrap_or_else(default_path);
    let reference = render();
    match mode.as_str() {
        "print" => print!("{reference}"),
        "write" => std::fs::write(&path, reference)?,
        "check" => {
            let current = std::fs::read_to_string(&path)?;
            if current != reference {
                eprintln!("{} is stale; run `make sync-docs`", path.display());
                report_first_difference(&current, &reference);
                std::process::exit(1);
            }
        }
        other => {
            return Err(format!("usage: docsgen [print|write|check] [PATH], got {other:?}").into());
        }
    }
    Ok(())
}

fn render() -> String {
    let options = MarkdownOptions::new().title("CLI reference".to_string());
    let command = normalize_defaults(pqbench_cli::Cli::command(), None);
    clap_markdown::help_markdown_command_custom(&command, &options)
}

/// Keep the reference independent of the machine that generated it.
///
/// `get table --fan-out` defaults to the machine's core count, so rendering the
/// command tree directly would put `12` on one machine and `4` on another. The
/// help text already says "cores"; render that instead of the number.
fn normalize_defaults(command: clap::Command, parent: Option<&str>) -> clap::Command {
    let name = command.get_name().to_owned();
    let command = if parent == Some("get") && name == "table" {
        command.mut_arg("fan_out", |arg| arg.default_value("cores"))
    } else {
        command
    };
    command.mut_subcommands(|sub| normalize_defaults(sub, Some(&name)))
}

/// Point at the first line that differs, so a stale-reference failure says
/// what changed instead of only that something did.
fn report_first_difference(current: &str, reference: &str) {
    let current_lines: Vec<&str> = current.lines().collect();
    let reference_lines: Vec<&str> = reference.lines().collect();
    for (index, (current_line, reference_line)) in
        current_lines.iter().zip(&reference_lines).enumerate()
    {
        if current_line != reference_line {
            eprintln!("first difference at line {}:", index + 1);
            eprintln!("  committed: {current_line:?}");
            eprintln!("  generated: {reference_line:?}");
            return;
        }
    }
    match current_lines.len().cmp(&reference_lines.len()) {
        std::cmp::Ordering::Less => {
            eprintln!("generated has extra line {}", current_lines.len() + 1);
        }
        std::cmp::Ordering::Greater => {
            eprintln!("committed has extra line {}", reference_lines.len() + 1);
        }
        std::cmp::Ordering::Equal => eprintln!("files differ only in trailing whitespace"),
    }
}

/// The committed reference, relative to the repository root.
fn default_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/reference/cli.md")
}
