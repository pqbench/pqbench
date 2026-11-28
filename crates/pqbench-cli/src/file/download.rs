//! `download`: copy a table file's bytes to a local path.

use std::path::{Path, PathBuf};

use clap::Args;
use futures_util::TryStreamExt;

use pqbench::v1;

use crate::parse;
use crate::CliError;

/// Arguments for `download`.
#[derive(Args)]
pub(crate) struct DownloadArgs {
    /// the file to write
    #[arg(long, value_name = "PATH")]
    output: PathBuf,
}

/// Copy each table-file record's blob to `--output`: one record per run, so
/// the path is the one the caller named.
pub(crate) async fn run(
    lake: &pqbench::Lake,
    daemon: &crate::daemon::Daemon,
    args: &DownloadArgs,
) -> Result<(), CliError> {
    let mut parents = parse::parents::<v1::TableFile>("download").await?;
    let Some(file) = parents.try_next().await? else {
        return Err("download reads one table-file record on standard input".into());
    };
    if parents.try_next().await?.is_some() {
        return Err("download writes one file: give it one table-file record".into());
    }
    let name = file
        .blob
        .as_deref()
        .ok_or("a table-file record needs a blob")?;
    let table = v1::name::parse_table_file(&file.name)
        .map(|(table, _)| table)
        .ok_or("a table-file record needs a catalogs/…/tables/… name")?;
    let env = daemon.resolve_env(&table).await?;
    let credentials = v1::Credentials::from_env(&env);
    let lake = lake.with_lease(Some(&credentials));
    download(&lake, name, &args.output).await?;
    Ok(())
}

/// Write the blob's bytes to `path`, in pieces, so memory stays bounded.
async fn download(lake: &pqbench::Lake, name: &str, path: &Path) -> Result<(), CliError> {
    let uri = v1::name::blob_uri(name).ok_or_else(|| format!("{name} is not a blobs/… name"))?;
    pqbench::third_party::object_store::copy(lake, &uri, path).await?;
    Ok(())
}
