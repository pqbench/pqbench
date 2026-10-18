//! `catalog`: the catalogs at the endpoint, and one catalog's record.

use std::path::PathBuf;

use clap::Args;

use crate::emit::Format;

pub(crate) mod get;
pub(crate) mod list;

/// Arguments for `get catalog`: the catalog name and the output flags.
#[derive(Args)]
pub(crate) struct NameArgs {
    /// catalog name; without it, read `Catalog` refs on stdin
    catalog: Option<String>,
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
