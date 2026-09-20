use clap::Args;
use pqbench::bytemass;
use serde::Deserialize;

use crate::CliError;

/// Arguments for `bytemass`.
#[derive(Args)]
pub(crate) struct BytemassArgs {
    /// parquet paths or glob masks; quote masks to prevent shell expansion
    #[arg(required_unless_present = "source", conflicts_with = "source")]
    inputs: Vec<String>,
    /// read the inputs from a source document on standard input (`-` only)
    #[arg(long, value_name = "-")]
    source: Option<String>,
    /// emit per-column byte masses as JSON instead of text stats
    #[arg(long = "json", conflicts_with = "d3")]
    json: bool,
    /// emit a self-contained d3 treemap HTML (open in a browser) instead of text stats
    #[arg(long = "d3")]
    d3: bool,
}

/// Build the typed request, measure, and render the CLI's chosen format. The
/// CLI owns the format decision; the library just returns the table.
pub(crate) fn run(args: &BytemassArgs) -> Result<(), CliError> {
    let request = bytemass::BytemassRequest {
        inputs: match &args.source {
            Some(source) => read_source(source)?,
            None => args.inputs.clone(),
        },
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let rows = runtime.block_on(bytemass::bytemass(&request))?;
    let output = if args.json {
        bytemass::render_json(&rows)?
    } else if args.d3 {
        bytemass::render_html(&rows)?
    } else {
        bytemass::render_text(&rows)?
    };
    print!("{output}");
    Ok(())
}

/// A versioned document naming the objects an external producer resolved.
/// Storage configuration stays in the `AWS_*` environment, as for any URI.
#[derive(Deserialize)]
struct RemoteSource {
    kind: String,
    version: u32,
    inputs: Vec<String>,
}

/// Read the inputs a producer piped in, as `pqbench.remote-source` version 1.
fn read_source(source: &str) -> Result<Vec<String>, CliError> {
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
    Ok(source.inputs)
}
