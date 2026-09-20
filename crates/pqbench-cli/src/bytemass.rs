use clap::Args;
use pqbench::bytemass;

use crate::CliError;

/// Arguments for `bytemass`.
#[derive(Args)]
pub(crate) struct BytemassArgs {
    /// parquet paths or glob masks; quote masks to prevent shell expansion
    #[arg(required = true)]
    inputs: Vec<String>,
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
    let request = bytemass::BytemassRequest {
        inputs: args.inputs.clone(),
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
