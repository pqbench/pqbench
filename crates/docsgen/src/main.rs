//! Generate the CLI reference from the pqbench clap tree.
//!
//! `docsgen` is a development tool: it depends on the CLI library for the
//! command definitions, but it is not part of the shipped binary. `write`
//! regenerates the committed page, `check` fails when it is stale, and `print`
//! writes it to stdout.

use std::path::{Path, PathBuf};

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
    clap_markdown::help_markdown_custom::<pqbench_cli::Cli>(&options)
}

/// The committed reference, relative to the repository root.
fn default_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/reference/cli.md")
}
