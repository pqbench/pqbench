use clap::Args;
use pqbench::bytemass;

use crate::CliError;

/// Arguments for `bytemass`.
#[derive(Args)]
pub(crate) struct BytemassArgs {
    /// parquet paths or glob masks; quote masks to prevent shell expansion
    #[arg(required = true)]
    inputs: Vec<String>,
    /// emit per-column byte masses as JSON instead of text stats
    #[arg(long = "json", conflicts_with = "d3")]
    json: bool,
    /// emit a self-contained d3 treemap HTML (open in a browser) instead of text stats
    #[arg(long = "d3")]
    d3: bool,
    /// reuse footer measurements when the object URI, size, and S3 ETag match
    #[arg(long, value_name = "DIR")]
    cache_dir: Option<std::path::PathBuf>,
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
    let rows = match &args.cache_dir {
        Some(directory) => {
            let cache = bytemass::FileMassCache::open(directory)?;
            runtime.block_on(bytemass::bytemass_with_cache(&request, &cache))?
        }
        None => runtime.block_on(bytemass::bytemass(&request))?,
    };
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
