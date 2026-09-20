use pqbench::lz;

use crate::bench::{bench_plan, BenchArgs};
use crate::CliError;

pub(crate) fn run(args: &BenchArgs) -> Result<(), CliError> {
    let plan = bench_plan(args)?;
    let raw = lz::bench_file(&args.file, &plan.codec_configs, plan.passes)?;
    lz::render(&lz::aggregate(&raw, &plan.stats_config));
    Ok(())
}
