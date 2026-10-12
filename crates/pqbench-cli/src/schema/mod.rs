//! `schema`: the schemas in a catalog, and one schema's record.

use std::path::PathBuf;

use clap::Args;

use crate::emit::Format;

pub(crate) mod get;
pub(crate) mod ls;

/// Arguments for `get schema`: the `catalog.schema` name and the output flags.
#[derive(Args)]
pub(crate) struct NameArgs {
    /// schema name (`catalog.schema`); without it, read schema refs on stdin
    schema: Option<String>,
    /// stdout format: auto (table on a terminal) | table | json
    #[arg(long, value_enum, default_value_t = Format::Auto)]
    format: Format,
    /// also write the lz4 NDJSON stream to FILE
    #[arg(short = 'o', long = "output", value_name = "FILE")]
    output: Option<PathBuf>,
    /// requests in flight at once
    #[arg(long, default_value_t = 64)]
    fan_out: usize,
}
