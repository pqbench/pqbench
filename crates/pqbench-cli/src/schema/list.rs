//! `list schemas`: the schemas in a catalog, one `Schema` line each.

use clap::Args;
use futures_util::stream;
use pqbench::v1;

use crate::parse;
use crate::source::{drive, flat_map};
use crate::CliError;

/// Arguments for `list schemas`.
#[derive(Args)]
pub(crate) struct ListArgs {
    /// catalogs in flight at once
    #[arg(long, default_value_t = 64)]
    fan_out: usize,
}

pub(crate) async fn run(daemon: &crate::daemon::Daemon, args: &ListArgs) -> Result<(), CliError> {
    let parents = parse::parents::<v1::Catalog>("list schemas").await?;
    let daemon = daemon.clone();
    drive(flat_map(
        parents,
        args.fan_out,
        move |catalog: v1::Catalog| {
            let daemon = daemon.clone();
            async move {
                let rows = daemon.list_schemas(&catalog).await?;
                Ok(stream::iter(
                    rows.into_iter().map(Ok::<_, std::convert::Infallible>),
                ))
            }
        },
    ))
    .await
}
