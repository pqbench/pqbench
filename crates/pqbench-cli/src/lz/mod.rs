//! `lz`: sweep codecs over a file and report speed and ratio.

use pqbench::lz::{self, LzRequest};

use crate::bench::BenchArgs;
use crate::emit::Emitter;
use crate::CliError;

pub(crate) async fn run(args: &BenchArgs) -> Result<(), CliError> {
    let request = LzRequest {
        file: args.file.clone(),
        codec_specs: args.codec_specs.clone(),
        samples: args.samples,
        warmup_iterations: args.warmup_iterations,
        mode: args.mode.into(),
    };
    let report = lz::lz(&request)?;
    let mut emit = Emitter::open();
    let blob = pqbench::v1::name::blob_name(&args.file.display().to_string());
    for row in &report.rows {
        emit.write(&crate::render::SweepRow {
            name: format!("{blob}/codecs/{}-{}", row.codec, row.level),
            row: row.clone(),
        })?;
    }
    emit.finish()
}
