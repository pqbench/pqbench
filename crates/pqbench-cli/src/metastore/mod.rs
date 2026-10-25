//! `metastore`: the endpoint's metastore record.

use clap::Args;

pub(crate) mod get;

/// Arguments for `get metastore`: the output flags.
#[derive(Args)]
pub(crate) struct OutputArgs {}
