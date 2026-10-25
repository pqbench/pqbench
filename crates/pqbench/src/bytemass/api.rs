//! The `bytemass` command: one typed request in, per-column byte masses out.
//!
//! The result is a `Vec<v1::Bytemass>`: one record per column, its chunks
//! folded over the file's row groups. The CLI streams those records;
//! `pqbench viz` collects them.

use std::collections::BTreeMap;

use crate::v1;
use crate::Lake;

use super::Error;

/// Arguments for the `bytemass` command.
#[derive(Debug, Clone, Default)]
pub struct BytemassRequest {
    /// Parquet paths or glob masks; quote masks to prevent shell expansion.
    pub inputs: Vec<String>,
    /// Storage options (`AWS_*` names) for remote inputs.
    pub env: BTreeMap<String, String>,
}

/// One measured file: its blob and one `Bytemass` per column.
#[derive(Debug, Clone)]
pub struct MeasuredFile {
    pub blob: v1::Blob,
    pub columns: Vec<v1::Bytemass>,
}

/// Measure the per-column byte masses of Parquet files.
///
/// Inputs are local paths or storage URIs, each possibly a local or S3 glob
/// mask. Only file footers are read. The result is one [`v1::Bytemass`] per
/// column, in input order.
///
/// # Errors
/// Fails when there are no inputs, a mask matches no files, an input cannot be
/// read, or a byte total overflows.
pub async fn bytemass(lake: &Lake, request: &BytemassRequest) -> Result<Vec<v1::Bytemass>, Error> {
    if request.inputs.is_empty() {
        return Err(Error::NoInputs);
    }
    Ok(measure_files(lake, request)
        .await?
        .into_iter()
        .flat_map(|file| file.columns)
        .collect())
}

/// Measure one file into its blob and its per-column masses.
///
/// The footer is read once; only file metadata is fetched, never data pages.
///
/// # Errors
/// Fails for an unreadable object, an invalid Parquet footer, or an
/// unsupported URI.
pub async fn measure_file(
    lake: &Lake,
    uri: &str,
    env: &BTreeMap<String, String>,
) -> Result<MeasuredFile, Error> {
    super::collection::measure_file(lake, uri, env).await
}

/// Measure each expanded input into a [`MeasuredFile`].
///
/// # Errors
/// Fails for empty inputs, unmatched patterns, invalid footers or I/O failures.
pub async fn measure_files(
    lake: &Lake,
    request: &BytemassRequest,
) -> Result<Vec<MeasuredFile>, Error> {
    if request.inputs.is_empty() {
        return Err(Error::NoInputs);
    }
    super::collection::measure_files(lake, &request.inputs, &request.env).await
}

/// Resolve local and S3 patterns to sorted, unique object paths.
///
/// # Errors
/// Invalid patterns, missing backend, listing errors, or unmatched patterns.
pub async fn expand_inputs(
    lake: &Lake,
    inputs: &[String],
    env: &BTreeMap<String, String>,
) -> Result<Vec<String>, Error> {
    Ok(super::collection::expand_inputs(lake, inputs, env)
        .await?
        .into_iter()
        .map(|path| path.to_string_lossy().into_owned())
        .collect())
}
