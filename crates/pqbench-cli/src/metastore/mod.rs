//! `metastore`: the endpoint's metastore record.

use std::path::PathBuf;

use clap::Args;

use crate::emit::Format;

pub(crate) mod get;

/// Arguments for `get metastore`: the output flags.
#[derive(Args)]
pub(crate) struct OutputArgs {
    /// stdout format: auto (table on a terminal) | table | json
    #[arg(long, value_enum, default_value_t = Format::Auto)]
    format: Format,
    /// also write the lz4 NDJSON stream to FILE
    #[arg(short = 'o', long = "output", value_name = "FILE")]
    output: Option<PathBuf>,
}
