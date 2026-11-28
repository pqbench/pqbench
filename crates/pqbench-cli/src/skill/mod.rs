//! `skill`: print the bundled agent recipes.

use std::io::Write;

use clap::Args;
use pqbench::skill::{self, Skill};
use serde::Serialize;

use crate::CliError;

/// Arguments for `skill`.
#[derive(Args)]
pub(crate) struct SkillArgs {
    /// skill name, or `name document` (`parquet-advisor recipes`)
    names: Vec<String>,
}

pub(crate) fn run(args: &SkillArgs) -> Result<(), CliError> {
    match args.names.as_slice() {
        [] => list(),
        [name] => show(name, ""),
        [name, document] => show(name, document),
        _ => Err("skill takes a name and optional document (parquet-advisor recipes)".into()),
    }
}

fn list() -> Result<(), CliError> {
    let mut out = std::io::stdout().lock();
    for item in skill::skills() {
        let record = SkillRecord {
            kind: "pqbench.skill",
            version: 1,
            name: item.name,
            description: item.description,
            documents: document_names(item),
        };
        let line = serde_json::to_string(&record)?;
        closed_pipe(writeln!(out, "{line}"))?;
    }
    Ok(())
}

fn show(name: &str, document: &str) -> Result<(), CliError> {
    let markdown = skill::document(name, document)?;
    let mut out = std::io::stdout().lock();
    closed_pipe(write_markdown(&mut out, markdown))
}

/// Write the document, ending it with a newline when it has none.
fn write_markdown(out: &mut impl Write, markdown: &str) -> std::io::Result<()> {
    out.write_all(markdown.as_bytes())?;
    if !markdown.ends_with('\n') {
        out.write_all(b"\n")?;
    }
    Ok(())
}

/// A reader that went away (`| head`) is not a failure, the way every other
/// command treats it.
fn closed_pipe(written: std::io::Result<()>) -> Result<(), CliError> {
    match written {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::BrokenPipe => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn document_names(skill: &Skill) -> Vec<&'static str> {
    skill
        .documents
        .iter()
        .map(|document| {
            if document.name.is_empty() {
                skill.name
            } else {
                document.name
            }
        })
        .collect()
}

#[derive(Serialize)]
struct SkillRecord {
    kind: &'static str,
    version: u32,
    name: &'static str,
    description: &'static str,
    documents: Vec<&'static str>,
}
