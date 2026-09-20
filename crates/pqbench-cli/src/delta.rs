use clap::Args;
use pqbench::table::delta::{self, DeltaRequest};

/// Arguments for Delta snapshot analysis.
#[derive(Args)]
pub(crate) struct DeltaArgs {
    /// local Delta table directory or table URI
    table: String,
    /// snapshot version; defaults to the latest version
    #[arg(long)]
    version: Option<u64>,
    /// emit the complete report as JSON
    #[arg(long = "json", conflicts_with = "is_d3")]
    is_json: bool,
    /// emit a self-contained d3 treemap HTML
    #[arg(long = "d3")]
    is_d3: bool,
}

pub(crate) fn run(args: &DeltaArgs) -> Result<(), crate::CliError> {
    let request = DeltaRequest {
        table: args.table.clone(),
        version: args.version,
    };
    let runtime = tokio::runtime::Runtime::new()?;
    let report = runtime.block_on(delta::delta(&request))?;
    let output = if args.is_json {
        delta::render_json(&report)?
    } else if args.is_d3 {
        delta::render_html(&report)?
    } else {
        delta::render_text(&report)?
    };
    print!("{output}");
    Ok(())
}
