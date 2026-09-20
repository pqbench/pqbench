use std::path::PathBuf;

use clap::Args;

/// Arguments for local Delta snapshot analysis.
#[derive(Args)]
pub(crate) struct DeltaArgs {
    /// local Delta table directory
    path: PathBuf,
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
    let runtime = tokio::runtime::Runtime::new()?;
    let report = runtime.block_on(pqbench::table::delta::read_local(&args.path, args.version))?;
    let output = if args.is_json {
        pqbench::table::delta::json(&report)?
    } else if args.is_d3 {
        pqbench::table::delta::render_html(&report)?
    } else {
        pqbench::table::delta::render(&report)?
    };
    print!("{output}");
    Ok(())
}
