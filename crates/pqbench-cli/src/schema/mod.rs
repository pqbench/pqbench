//! `schema`: the schemas in a catalog, and one schema's record.

use clap::Args;

pub(crate) mod get;
pub(crate) mod list;

/// Arguments for `get schema`: the `catalog.schema` name and the output flags.
#[derive(Args)]
pub(crate) struct NameArgs {
    /// schema name (`catalog.schema`); without it, read schema refs on stdin
    schema: Option<String>,
    /// requests in flight at once
    #[arg(long, default_value_t = 64)]
    fan_out: usize,
}
