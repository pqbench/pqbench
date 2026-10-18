//! `get catalog`: one catalog's record.

use futures_util::stream::StreamExt;
use pqbench::catalog::get;

use super::NameArgs;
use crate::emit::Emitter;
use crate::source::{self, catalog_ref, read_input, Input};
use crate::CliError;

pub(crate) async fn run(workspace: &pqbench::Workspace, args: &NameArgs) -> Result<(), CliError> {
    let input = read_input("get catalog").await?;
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    let catalogs = match &args.catalog {
        Some(name) => {
            if input.first.is_some() {
                return Err("get catalog takes CATALOG or a catalog stream, not both".into());
            }
            get_one(workspace, name, &mut emit).await?
        }
        None => get_stream(workspace, input, args.fan_out, &mut emit).await?,
    };
    emit.finish(&format!("catalogs: {catalogs}\n")).await
}

/// Read one named catalog's record.
async fn get_one(
    workspace: &pqbench::Workspace,
    name: &str,
    emit: &mut Emitter,
) -> Result<usize, CliError> {
    let catalog = get::get(workspace, &pqbench::v1::name::catalog(name)).await?;
    emit.write_row(&catalog).await?;
    Ok(1)
}

/// Read each catalog's record from a catalog stream.
async fn get_stream(
    workspace: &pqbench::Workspace,
    input: Input,
    fan_out: usize,
    emit: &mut Emitter,
) -> Result<usize, CliError> {
    if !input.piped {
        return Err("get catalog needs CATALOG or a catalog stream".into());
    }
    let records = source::records("get catalog", input.first, input.lines);
    let mut catalogs = 0;
    let mut reads = records
        .map(|record| async {
            let name = catalog_ref("get catalog", &record?)?;
            Ok::<_, CliError>(get::get(workspace, &name).await?)
        })
        .buffer_unordered(fan_out.max(1));
    while let Some(catalog) = reads.next().await {
        emit.write_row(&catalog?).await?;
        catalogs += 1;
    }
    Ok(catalogs)
}
