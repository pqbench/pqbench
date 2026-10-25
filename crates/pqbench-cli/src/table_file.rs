//! `table-file`: name storage paths as the file records the walk consumes.

use clap::Args;
use futures_util::stream::{self, StreamExt};
use pqbench::v1;

use crate::source::{drive, Records};
use crate::CliError;

/// Arguments for `table-file`.
#[derive(Args)]
pub(crate) struct TableFileArgs {
    /// Parquet paths or storage URIs
    #[arg(required = true)]
    paths: Vec<String>,
}

/// Emit one table-file record per path, under the `local` catalog.
pub(crate) async fn run(args: &TableFileArgs) -> Result<(), CliError> {
    let rows: Records<'_, v1::TableFile> =
        stream::iter(args.paths.iter().map(|path| Ok(table_file(path)))).boxed_local();
    drive(rows).await
}

/// The table-file a path names: its file id, and its blob.
fn table_file(path: &str) -> v1::TableFile {
    let file = path
        .rsplit('/')
        .find(|segment| !segment.is_empty())
        .unwrap_or(path);
    v1::TableFile {
        name: v1::name::table_file_name(&v1::name::local_table(path), file),
        blob: Some(v1::name::blob_name(path)),
        ..v1::TableFile::default()
    }
}
