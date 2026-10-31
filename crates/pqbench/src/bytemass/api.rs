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
    /// Parquet paths or storage URIs.
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
/// Inputs are local paths or storage URIs. Only file footers are read. The
/// result is one [`v1::Bytemass`] per column, in input order.
///
/// # Errors
/// Fails when there are no inputs, an input cannot be read, or a byte total
/// overflows.
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

/// Measure the file a table-file record names.
///
/// The record's blob is its storage location, its credentials are the lease
/// the read uses, its size (when nonzero) is the log's claim, checked against
/// the footer, and its `sample_rate` (when the record was sampled) is copied
/// onto the blob and every column, so a sum over the stream can weight them.
///
/// # Errors
/// Fails when the record names no blob, the blob name is invalid, the file
/// cannot be read, or its footer size differs from the log.
pub async fn measure(lake: &Lake, file: &v1::TableFile) -> Result<MeasuredFile, Error> {
    let blob = file
        .blob
        .as_deref()
        .ok_or_else(|| Error::Invalid("a table-file record needs a blob to read".into()))?;
    let table = v1::name::parse_table_file(&file.name)
        .map(|(table, _)| table)
        .ok_or_else(|| {
            Error::Invalid("a table-file record needs a catalogs/…/tables/… name".into())
        })?;
    let uri = v1::name::blob_uri(blob)
        .ok_or_else(|| Error::Invalid(format!("not a blob name: {blob}")))?;
    let env = file
        .credentials
        .as_ref()
        .map(v1::Credentials::to_env)
        .unwrap_or_default();
    let mut measured = measure_file(lake, &uri, &env).await?;
    if let Some(size) = file.size_bytes.filter(|size| *size != 0) {
        if measured.blob.size_bytes != Some(size) {
            return Err(Error::Invalid(format!(
                "active file size differs from log: {uri} (expected {size}, found {:?})",
                measured.blob.size_bytes
            )));
        }
    }
    measured.blob.sample_rate = file.sample_rate;
    for column in &mut measured.columns {
        column.table = Some(table.clone());
        column.sample_rate = file.sample_rate;
    }
    Ok(measured)
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
/// Fails for empty inputs, invalid footers or I/O failures.
pub async fn measure_files(
    lake: &Lake,
    request: &BytemassRequest,
) -> Result<Vec<MeasuredFile>, Error> {
    if request.inputs.is_empty() {
        return Err(Error::NoInputs);
    }
    super::collection::measure_files(lake, &request.inputs, &request.env).await
}
