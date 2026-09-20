use pqbench::lz;

use crate::bench::BenchArgs;
use crate::CliError;

pub(crate) fn run(args: &BenchArgs) -> Result<(), CliError> {
    let report = lz::lz(&args.request())?;
    let output = if args.is_json {
        lz::render_json(&report)?
    } else {
        lz::render_text(&report)
    };
    print!("{output}");
    Ok(())
}
