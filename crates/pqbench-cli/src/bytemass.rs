use clap::Args;
use pqbench::bytemass;
#[cfg(feature = "aws")]
use serde::Deserialize;

use crate::CliError;

/// Arguments for `bytemass`.
#[derive(Args)]
pub(crate) struct BytemassArgs {
    /// parquet paths or glob masks; quote masks to prevent shell expansion
    #[arg(required_unless_present = "source", conflicts_with = "source")]
    inputs: Vec<String>,
    /// versioned pqbench remote-source JSON read from standard input (`-` only)
    #[arg(long, value_name = "-")]
    source: Option<String>,
    /// emit the byte-mass tree as JSON (composable) instead of text stats
    #[arg(long = "json", conflicts_with = "is_d3")]
    is_json: bool,
    /// emit a self-contained d3 treemap HTML (open in a browser) instead of text stats
    #[arg(long = "d3")]
    is_d3: bool,
}

/// Build the typed request, measure, and render the CLI's chosen format. The
/// CLI owns the format decision; the library just returns the table.
pub(crate) fn run(args: &BytemassArgs) -> Result<(), CliError> {
    let request = match &args.source {
        Some(source) => remote_source_request(source)?,
        None => bytemass::BytemassRequest {
            inputs: args.inputs.clone(),
            ..Default::default()
        },
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let rows = runtime.block_on(bytemass::bytemass(&request))?;
    let output = if args.is_json {
        bytemass::render_json(&rows)?
    } else if args.is_d3 {
        bytemass::render_html(&rows)?
    } else {
        bytemass::render_text(&rows)?
    };
    print!("{output}");
    Ok(())
}

/// Stdin protocol for external catalog/credential producers.
#[cfg(feature = "aws")]
#[derive(Deserialize)]
struct RemoteSource {
    kind: String,
    version: u32,
    inputs: Vec<String>,
    #[serde(default)]
    object_store_options: std::collections::BTreeMap<String, String>,
}

#[cfg(feature = "aws")]
fn remote_source_request(source: &str) -> Result<bytemass::BytemassRequest, CliError> {
    if source != "-" {
        return Err(
            "--source accepts only `-` (a remote-source JSON document on standard input)".into(),
        );
    }
    let source: RemoteSource = serde_json::from_reader(std::io::stdin().lock())
        .map_err(|error| format!("invalid pqbench remote source document: {error}"))?;
    if source.kind != "pqbench.remote-source" || source.version != 1 {
        return Err(
            "unsupported remote source; expected kind `pqbench.remote-source` version 1".into(),
        );
    }
    if source.inputs.is_empty() {
        return Err("remote source contains no inputs".into());
    }
    if source.inputs.iter().any(|input| !input.contains("://")) {
        return Err("remote source inputs must be absolute URIs".into());
    }
    Ok(bytemass::BytemassRequest {
        inputs: source.inputs,
        object_store_options: source.object_store_options.into_iter().collect(),
    })
}

#[cfg(not(feature = "aws"))]
fn remote_source_request(_: &str) -> Result<bytemass::BytemassRequest, CliError> {
    Err("--source requires the `aws` feature".into())
}
