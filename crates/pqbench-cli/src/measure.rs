//! Measure a table-file record: its blob, and its columns with the walk's
//! context on each (table, sample rate, file size, storage class).
//!
//! `list columns` and `list suggestions` both start here, so the sample-rate
//! check and the enrichment live in one place.

use pqbench::{bytemass, v1};

use crate::CliError;

/// Measure the file a table-file record names, enriched for the walk.
///
/// # Errors
/// Fails when the record has no blob, its name is not a table-file name, its
/// sample rate is outside `(0, 1]`, or the footer cannot be read.
pub(crate) async fn table_file(
    lake: &pqbench::Lake,
    daemon: &crate::daemon::Daemon,
    file: &v1::TableFile,
) -> Result<bytemass::MeasuredFile, CliError> {
    let name = file
        .blob
        .as_deref()
        .ok_or_else(|| "a table-file record needs a blob to read".to_string())?;
    let table = v1::name::parse_table_file(&file.name)
        .map(|(table, _)| table)
        .ok_or_else(|| "a table-file record needs a catalogs/…/tables/… name".to_string())?;
    if let Some(rate) = file.sample_rate {
        if !(rate > 0.0 && rate <= 1.0) {
            return Err(format!("a table file's sample rate must be in (0, 1], not {rate}").into());
        }
    }
    let env = daemon.resolve_env(&table).await?;
    let credentials = v1::Credentials::from_env(&env);
    let lake = lake.with_lease(Some(&credentials));
    let measured = bytemass::measure_file(&lake, name).await?;
    let mut blob = measured.blob;
    let mut columns = measured.columns;
    blob.sample_rate = file.sample_rate;
    for column in &mut columns {
        column.table = Some(table.clone());
        column.sample_rate = file.sample_rate;
        column.size_bytes = blob.size_bytes;
        column.storage_class = blob.storage_class.clone();
    }
    Ok(bytemass::MeasuredFile { blob, columns })
}
