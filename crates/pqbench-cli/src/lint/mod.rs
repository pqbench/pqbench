//! `list suggestions` and `get suggestion`: the storage a local blob's values
//! do not need.

use std::path::{Path, PathBuf};

use clap::Args;
use futures_util::stream::{self, StreamExt};
use pqbench::v1;

use crate::parse;
use crate::source::{drive, flat_map, map, Records};
use crate::table_file;
use crate::CliError;

const LIST_COMMAND: &str = "list suggestions";
const GET_COMMAND: &str = "get suggestion";

/// Arguments for `list suggestions`.
#[derive(Args)]
pub(crate) struct SuggestionsArgs {
    /// local parquet files to lint, when there is no table-file stream to
    /// read; each path becomes the table-file record a `table-file` stage
    /// would name
    #[arg(value_name = "PATH")]
    paths: Vec<PathBuf>,
    /// read each file from this path instead of the blob, keeping the
    /// record's name
    #[arg(long, value_name = "PATH")]
    local: Option<PathBuf>,
}

/// Arguments for `get suggestion`.
#[derive(Args)]
pub(crate) struct VerifyArgs {
    /// read the file from this path instead of the blob, keeping the
    /// record's name
    #[arg(long, value_name = "PATH")]
    local: Option<PathBuf>,
}

/// Emit each table file's potential suggestions as its record arrives.
pub(crate) async fn run(args: &SuggestionsArgs) -> Result<(), CliError> {
    let parents = table_files(&args.paths).await?;
    let local = args.local.as_deref();
    // One file at a time: decoding a sample is CPU-bound, so overlapping files
    // buys nothing and holds more samples in memory.
    let records = flat_map(parents, 1, move |file: v1::TableFile| async move {
        let blob = file
            .blob
            .as_deref()
            .ok_or("a table-file record needs a blob to read")?;
        let path = read_path(blob, local)?;
        let suggestions = pqbench::lint::suggestions(&path, &file)?;
        Ok(stream::iter(suggestions.into_iter().map(Ok::<_, CliError>)))
    });
    drive(records).await
}

/// The table files to lint: the paths given, or the records on standard input
/// when there are none.
async fn table_files(paths: &[PathBuf]) -> Result<Records<'static, v1::TableFile>, CliError> {
    if paths.is_empty() {
        return parse::parents::<v1::TableFile>(LIST_COMMAND).await;
    }
    let records: Vec<Result<v1::TableFile, CliError>> =
        paths.iter().map(|path| path_record(path)).collect();
    Ok(stream::iter(records).boxed_local())
}

/// A local path as the record `pqbench table-file` names, with the size the
/// file has now: the same minimal record, so a path lints under the blob name
/// the file's bytes have.
fn path_record(path: &Path) -> Result<v1::TableFile, CliError> {
    let text = path
        .to_str()
        .ok_or_else(|| format!("{} is not a UTF-8 path", path.display()))?;
    let size_bytes = std::fs::metadata(path)
        .map_err(|error| format!("{}: {error}", path.display()))?
        .len();
    Ok(v1::TableFile {
        size_bytes: Some(size_bytes),
        ..table_file::table_file(text)
    })
}

/// Measure each potential suggestion and emit the same record enriched with
/// the control's verdict.
pub(crate) async fn verify(args: &VerifyArgs) -> Result<(), CliError> {
    let parents = parse::parents::<v1::Suggestion>(GET_COMMAND).await?;
    let local = args.local.as_deref();
    // One suggestion at a time: measuring re-encodes the sample, which is
    // CPU-bound, so overlap buys nothing.
    let records = map(parents, 1, move |candidate: v1::Suggestion| async move {
        let (service, locator, _, _) = v1::Suggestion::parse_name(&candidate.name)
            .ok_or_else(|| format!("not a suggestion name: {}", candidate.name))?;
        let blob = v1::name::blob(&service, &locator);
        let path = read_path(&blob, local)?;
        Ok::<_, CliError>(pqbench::lint::verify(&path, &candidate)?)
    });
    drive(records).await
}

/// The path a blob's data is read from: the local copy when the caller has
/// one, else the file a `file` blob names.
fn read_path(blob: &str, local: Option<&Path>) -> Result<PathBuf, CliError> {
    if let Some(local) = local {
        return Ok(local.to_path_buf());
    }
    let (service, locator) =
        v1::name::parse_blob(blob).ok_or_else(|| format!("not a blob name: {blob}"))?;
    if service != "file" {
        return Err(format!("{blob} is not a local file; give --local with a copy of it").into());
    }
    Ok(PathBuf::from(locator))
}
