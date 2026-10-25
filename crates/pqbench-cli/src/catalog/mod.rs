//! `catalog`: the catalogs at the endpoint, and one catalog's record.

use clap::Args;

pub(crate) mod get;
pub(crate) mod list;

/// Arguments for `get catalog`: the catalog name and the output flags.
#[derive(Args)]
pub(crate) struct NameArgs {
    /// requests in flight at once
    #[arg(long, default_value_t = 64)]
    fan_out: usize,
}
