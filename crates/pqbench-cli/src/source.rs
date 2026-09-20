//! The `--source -` document, shared by the commands that accept one.
//!
//! A producer resolves names in a catalog and pqbench measures bytes; this is
//! the seam between them, so nothing here knows about catalogs and pqbench
//! keeps no catalog dependency.

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::CliError;

/// A versioned document naming what an external producer resolved, plus the
/// storage environment to read it with — a catalog that vends expiring
/// credentials can put them here instead of exporting them around the pipe.
#[derive(Deserialize)]
struct RemoteSource {
    kind: String,
    version: u32,
    inputs: Vec<String>,
    #[serde(default)]
    env: BTreeMap<String, String>,
}

/// Read the objects a producer piped in, as `pqbench.remote-source` version 1.
pub(crate) fn read_source_inputs(source: &str) -> Result<Vec<String>, CliError> {
    Ok(read_source(source)?.inputs)
}

/// Read a document naming one table, for commands that measure a snapshot.
#[cfg(feature = "delta")]
pub(crate) fn read_source_table(source: &str) -> Result<String, CliError> {
    let mut inputs = read_source(source)?.inputs;
    if inputs.len() > 1 {
        return Err(format!(
            "a Delta snapshot is one table, but the source document names {} inputs",
            inputs.len()
        )
        .into());
    }
    Ok(inputs.remove(0))
}

/// Parse the document from standard input and apply the environment it carries.
fn read_source(source: &str) -> Result<RemoteSource, CliError> {
    if source != "-" {
        return Err("--source accepts only `-` (a source document on standard input)".into());
    }
    let source: RemoteSource = serde_json::from_reader(std::io::stdin().lock())
        .map_err(|error| format!("invalid pqbench source document: {error}"))?;
    if source.kind != "pqbench.remote-source" || source.version != 1 {
        return Err(
            "unsupported source document; expected kind `pqbench.remote-source` version 1".into(),
        );
    }
    if source.inputs.is_empty() {
        return Err("source document contains no inputs".into());
    }
    for (key, value) in &source.env {
        if !key.starts_with("AWS_") {
            return Err(
                format!("source document may only set AWS_* variables, not `{key}`").into(),
            );
        }
        std::env::set_var(key, value);
    }
    Ok(source)
}
