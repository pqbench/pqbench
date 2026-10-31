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
    let markdown = clap_markdown::help_markdown_command_custom(&command, &options);
    preserve_help_layout(&markdown)
}

/// Keep the alignment of the free-form help blocks.
///
/// `clap-markdown` writes `long_about` and `after_help` verbatim. A block that
/// aligns columns with spaces becomes one run-on paragraph in Markdown, so wrap
/// each such paragraph in a fenced `text` block; plain prose stays prose.
fn preserve_help_layout(markdown: &str) -> String {
    let lines: Vec<&str> = markdown.lines().collect();
    let mut out = String::with_capacity(markdown.len() + 256);
    let mut index = 0;
    while index < lines.len() {
        if lines[index].starts_with("## `") {
            out.push_str(lines[index]);
            out.push_str("\n\n");
            index += 1;
            let start = index;
            while index < lines.len() && !lines[index].starts_with("**Usage:**") {
                index += 1;
            }
            write_help_block(&mut out, &lines[start..index]);
            out.push('\n');
            if index < lines.len() {
                out.push_str(lines[index]);
                out.push_str("\n\n");
                index += 1;
            }
            let start = index;
            while index < lines.len()
                && !lines[index].starts_with("######")
                && !lines[index].starts_with("## `")
            {
                index += 1;
            }
            write_help_block(&mut out, &lines[start..index]);
            out.push('\n');
            continue;
        }
        out.push_str(lines[index]);
        out.push('\n');
        index += 1;
    }
    out
}

fn write_help_block(out: &mut String, lines: &[&str]) {
    let mut paragraphs: Vec<Vec<&str>> = Vec::new();
    let mut current: Vec<&str> = Vec::new();
    for line in lines {
        if line.trim().is_empty() {
            if !current.is_empty() {
                paragraphs.push(std::mem::take(&mut current));
            }
        } else {
            current.push(line);
        }
    }
    if !current.is_empty() {
        paragraphs.push(current);
    }

    let aligned = |paragraph: &[&str]| {
        paragraph
            .iter()
            .any(|line| line.starts_with("  ") && !line.trim().is_empty())
    };
    let has_command = |paragraph: &[&str]| {
        paragraph
            .iter()
            .any(|line| line.split_whitespace().any(|word| word.starts_with("pqb")))
    };

    let mut index = 0;
    while index < paragraphs.len() {
        if index > 0 {
            out.push('\n');
        }
        if aligned(&paragraphs[index]) {
            let mut end = index;
            while end < paragraphs.len() && aligned(&paragraphs[end]) {
                end += 1;
            }
            let fence = if paragraphs[index..end].iter().any(|p| has_command(p)) {
                "console no-run"
            } else {
                "text"
            };
            out.push_str("```");
            out.push_str(fence);
            out.push('\n');
            for (offset, paragraph) in paragraphs[index..end].iter().enumerate() {
                if offset > 0 {
                    out.push('\n');
                }
                for line in paragraph {
                    out.push_str(line);
                    out.push('\n');
                }
            }
            out.push_str("```\n");
            index = end;
        } else {
            for line in &paragraphs[index] {
                out.push_str(line);
                out.push('\n');
            }
            index += 1;
        }
    }
}

/// Keep the reference independent of the machine that generated it.
///
/// `get table --fan-out` defaults to the machine's core count, so rendering the
/// command tree directly would put `12` on one machine and `4` on another. The
/// help text already says "cores"; render that instead of the number.
fn normalize_defaults(command: clap::Command, parent: Option<&str>) -> clap::Command {
    let name = command.get_name().to_owned();
    let command = if parent == Some("get") && name == "table" {
        command.mut_arg("fan_out", |arg| arg.default_value("the number of cores"))
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

#[cfg(test)]
mod tests {
    use super::preserve_help_layout;

    #[test]
    fn reference_renders_the_core_count_symbolically() {
        let reference = super::render();
        assert!(
            reference.contains("tables in flight at once (default: cores)"),
            "the fan-out help line is missing"
        );
        assert!(
            reference.contains("Default value: `the number of cores`"),
            "the fan-out default is not normalized"
        );
    }

    #[test]
    fn wraps_aligned_help_and_leaves_prose_alone() {
        let input = "## `pqbench`\n\nprose\n\n  aligned\n  rows\n\n**Usage:** `pqbench`\n\nExamples:\n  pqbench x\n\n###### **Options:**\n";
        let output = preserve_help_layout(input);
        assert!(
            output.contains("```text\n  aligned\n  rows\n```"),
            "{output}"
        );
        assert!(
            output.contains("```console no-run\nExamples:\n  pqbench x\n```"),
            "{output}"
        );
        assert!(!output.contains("```text\nprose\n```"), "{output}");
    }
}
