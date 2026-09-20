use std::path::PathBuf;

use clap::Args as ClapArgs;

/// Arguments for local Delta snapshot analysis.
#[derive(ClapArgs)]
pub(crate) struct Args {
    /// local Delta table directory
    path: PathBuf,
    /// snapshot version; defaults to the latest version
    #[arg(long)]
    version: Option<u64>,
    /// emit the complete report as JSON
    #[arg(long, conflicts_with = "d3")]
    json: bool,
    /// emit a self-contained d3 treemap HTML
    #[arg(long)]
    d3: bool,
}

pub(crate) fn run(args: &Args) -> Result<(), crate::CliError> {
    let runtime = tokio::runtime::Runtime::new()?;
    let report = runtime.block_on(pqbench::table::delta::read_local(&args.path, args.version))?;
    let output = if args.json {
        pqbench::table::delta::json(&report)?
    } else if args.d3 {
        pqbench::table::delta::render_html(&report)?
    } else {
        pqbench::table::delta::render(&report)?
    };
    print!("{output}");
    Ok(())
}
