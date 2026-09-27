//! The walk's input: the `pqbench.lake-source` a metadata command reads.
//!
//! `metastore` and `catalog` resolve their endpoint and bearer from the same
//! document; the command name only shapes the error messages.

use std::io::IsTerminal;

use serde::Deserialize;

use crate::CliError;

/// The endpoint and bearer a metadata command runs under.
#[derive(Debug, Clone)]
pub(crate) struct Source {
    pub endpoint: String,
    pub token: Option<String>,
}

#[derive(Deserialize)]
struct Document {
    version: u32,
    endpoint: String,
    #[serde(default)]
    token: Option<String>,
}

/// Read the `pqbench.lake-source` document on standard input.
pub(crate) async fn read_source(command: &str) -> Result<Source, CliError> {
    if std::io::stdin().is_terminal() {
        return Err(format!("{command} needs a pqbench.lake-source on standard input").into());
    }
    let mut bytes = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut tokio::io::stdin(), &mut bytes).await?;
    if bytes.iter().all(u8::is_ascii_whitespace) {
        return Err(format!("{command} reads a pqbench.lake-source document").into());
    }
    let source: Document = serde_json::from_slice(&bytes)
        .map_err(|error| format!("{command} reads a pqbench.lake-source document: {error}"))?;
    if source.version != 1 {
        return Err(
            "unsupported lake source; expected kind `pqbench.lake-source` version 1".into(),
        );
    }
    if source.endpoint.trim().is_empty() {
        return Err("lake source needs an endpoint".into());
    }
    Ok(Source {
        endpoint: source.endpoint,
        token: source.token,
    })
}
