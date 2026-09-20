use pqbench::compression;
use pqbench::parquet_helpers::{default_parser, PageParser};

use crate::bench::{bench_plan, BenchArgs};
use crate::CliError;

/// Read a NONE-compressed parquet file, parse its pages, and sweep every config
/// over them. This is `compression` wired end-to-end.
pub(crate) fn run(args: &BenchArgs) -> Result<(), CliError> {
    let plan = bench_plan(args)?;
    let bytes = std::fs::read(&args.file)?;
    let parsed = default_parser().parse_pages(&bytes)?;
    let raw = compression::bench_file(&parsed, &plan.configs, plan.passes)?;
    compression::render(
        &compression::aggregate(&raw, &plan.cfg, args.per_column),
        args.per_column,
    );
    Ok(())
}
