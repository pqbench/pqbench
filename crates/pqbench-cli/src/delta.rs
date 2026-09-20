use clap::Args;
use pqbench::bytemass::FileMassCache;
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
    #[arg(long = "json", conflicts_with = "d3")]
    json: bool,
    /// emit a self-contained d3 treemap HTML
    #[arg(long = "d3")]
    d3: bool,
    /// reuse footer measurements when the object URI, size, and S3 ETag match
    #[arg(long, value_name = "DIR")]
    cache_dir: Option<std::path::PathBuf>,
}

pub(crate) fn run(args: &DeltaArgs) -> Result<(), crate::CliError> {
    let request = DeltaRequest {
        table: args.table.clone(),
        version: args.version,
    };
    let runtime = tokio::runtime::Runtime::new()?;
    let report = match &args.cache_dir {
        Some(directory) => {
            let cache = FileMassCache::open(directory)?;
            runtime.block_on(delta::delta_with_cache(&request, &cache))?
        }
        None => runtime.block_on(delta::delta(&request))?,
    };
    let output = if args.json {
        delta::render_json(&report)?
    } else if args.d3 {
        delta::render_html(&report)?
    } else {
        delta::render_text(&report)?
    };
    print!("{output}");
    Ok(())
}
