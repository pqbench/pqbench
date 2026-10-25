//! `profile`: decode a row sample into per-column facts.

use clap::Args;
use pqbench::profile::{self, ProfileRequest};
use pqbench::third_party::parquet::api::read_sample;

use crate::emit::Emitter;
use crate::rows::{parse_rows, RowLimit};
use crate::CliError;

/// Arguments for `profile`: read and decode a bounded row sample, then stream
/// one `pqbench.profile-column` line per column.
#[derive(Args)]
pub(crate) struct ProfileArgs {
    /// parquet paths to sample
    inputs: Vec<String>,
    /// column name glob, repeatable; default: every column
    #[arg(long = "columns", value_name = "GLOB")]
    columns: Vec<String>,
    /// how many leading rows to read: `all` or `first:N`
    #[arg(long = "rows", value_parser = parse_rows, default_value = "first:8192", value_name = "METHOD")]
    row_limit: RowLimit,
    /// number of top values per column
    #[arg(long = "top", default_value_t = 8)]
    top: u32,
}

pub(crate) async fn run(args: &ProfileArgs) -> Result<(), CliError> {
    if args.inputs.is_empty() {
        return Err("profile needs parquet files".into());
    }
    let max_rows = args.row_limit.max_rows();
    let request = ProfileRequest {
        columns: args.columns.clone(),
        top: args.top,
    };
    let mut emit = Emitter::open();
    for input in &args.inputs {
        let sample = read_sample(std::path::Path::new(input), max_rows)?;
        let report = profile::profile(&sample, &request)?;
        let blob = pqbench::v1::name::blob_name(input);
        for column in &report.columns {
            emit.write(&crate::render::ProfileRow {
                name: format!("{blob}/columns/{}/profile", column.column),
                id: input.clone(),
                profile: column.clone(),
            })
            .await?;
        }
    }
    emit.finish().await
}
