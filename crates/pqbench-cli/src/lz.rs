use pqbench::lz::{self, LzRequest};

use crate::bench::BenchArgs;
use crate::CliError;

pub(crate) fn run(args: &BenchArgs) -> Result<(), CliError> {
    let request = LzRequest {
        file: args.file.clone(),
        codec_specs: args.codec_specs.clone(),
        samples: args.samples,
        warmup_iterations: args.warmup_iterations,
        mode: args.mode.into(),
    };
    let report = lz::lz(&request)?;
    let output = if args.is_json {
        lz::render_json(&report)?
    } else {
        lz::render_text(&report)
    };
    print!("{output}");
    Ok(())
}
