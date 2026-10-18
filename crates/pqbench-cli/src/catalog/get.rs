//! `get catalog`: one catalog's record.

use futures_util::stream::StreamExt;
use pqbench::catalog::get;

use super::NameArgs;
use crate::emit::Emitter;
use crate::source::{self, catalog_ref, read_input};
use crate::CliError;

pub(crate) async fn run(workspace: &pqbench::Workspace, args: &NameArgs) -> Result<(), CliError> {
    let input = read_input("get catalog").await?;
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    if let Some(name) = &args.catalog {
        if input.first.is_some() {
            return Err("get catalog takes CATALOG or a catalog stream, not both".into());
        }
        let catalog = get::read(workspace, &pqbench::v1::name::catalog(name)).await?;
        emit.write_row(&catalog).await?;
        return emit.finish("catalogs: 1\n").await;
    }
    if !input.piped {
        return Err("get catalog needs CATALOG or a catalog stream".into());
    }
    let records = source::records("get catalog", input.first, input.lines);
    let mut catalogs = 0;
    let mut reads = records
        .map(|record| async {
            let name = catalog_ref("get catalog", &record?)?;
            Ok::<_, CliError>(get::read(workspace, &name).await?)
        })
        .buffer_unordered(args.fan_out.max(1));
    while let Some(catalog) = reads.next().await {
        emit.write_row(&catalog?).await?;
        catalogs += 1;
    }
    emit.finish(&format!("catalogs: {catalogs}\n")).await
}
