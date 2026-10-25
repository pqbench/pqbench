//! `bytemass`: read a Parquet footer and emit per-column byte masses.

use std::collections::BTreeMap;

use clap::Args;
use futures_util::stream::{self, BoxStream, StreamExt};
use pqbench::bytemass;
use pqbench::v1;
use serde::Serialize;

use crate::document::{self, Record};
use crate::emit::Emitter;
use crate::error::{context, fail_when_dropped};
use crate::source::stage;
use crate::CliError;

/// Arguments for `bytemass`.
#[derive(Args)]
pub(crate) struct BytemassArgs {
    /// a Parquet path or storage URI (a glob expands); omit to read records
    /// on standard input
    file: Option<String>,
    /// files in flight at once
    #[arg(long, default_value_t = 128)]
    fan_out: usize,
}

/// Read the files the argument or the records name, and stream each record as
/// it is ready.
pub(crate) async fn run(lake: &pqbench::Lake, args: &BytemassArgs) -> Result<(), CliError> {
    let mut emit = Emitter::open();
    let inputs = match &args.file {
        Some(file) => path_inputs(lake, file).await?,
        None => document_inputs(lake, args.fan_out).await?,
    };
    let dropped = stage(inputs, args.fan_out, &mut emit, |input: Input| async move {
        measure(lake, input).await
    })
    .await?;
    emit.finish().await?;
    fail_when_dropped(dropped)
}

/// One file to read.
struct Input {
    /// The file's location.
    uri: String,
    /// The storage environment to read it with.
    env: BTreeMap<String, String>,
    /// The size the record claimed, when it carried one.
    size_bytes: Option<u64>,
}

/// A path or glob as files: local and S3 patterns expand here.
async fn path_inputs(
    lake: &pqbench::Lake,
    file: &str,
) -> Result<BoxStream<'static, Result<Input, CliError>>, CliError> {
    let files = [file.to_string()];
    let paths = bytemass::expand_inputs(lake, &files, &BTreeMap::new()).await?;
    Ok(stream::iter(paths.into_iter().map(|uri| {
        Ok(Input {
            uri,
            env: BTreeMap::new(),
            size_bytes: None,
        })
    }))
    .boxed())
}

/// A document's records as files: each record expands with `fan_out` of them
/// in flight, and a record that cannot be expanded becomes an item of its own,
/// so the stage reports it and carries on.
async fn document_inputs<'a>(
    lake: &'a pqbench::Lake,
    fan_out: usize,
) -> Result<BoxStream<'a, Result<Input, CliError>>, CliError> {
    let records = document::records("-").await?;
    Ok(records
        .map(move |record| async move { record_inputs(lake, record?).await })
        .buffer_unordered(fan_out.max(1))
        .flat_map(|inputs| match inputs {
            Ok(inputs) => stream::iter(inputs.into_iter().map(Ok::<_, CliError>)).boxed(),
            Err(error) => stream::iter([Err(error)]).boxed(),
        })
        .boxed())
}

/// The files one record names: a table-file is its blob and lease, a blob is
/// its location, and a remote source is its inputs, expanded with its env.
async fn record_inputs(lake: &pqbench::Lake, record: Record) -> Result<Vec<Input>, CliError> {
    match record {
        Record::File(file) => Ok(vec![file_input(file)?]),
        Record::Blob(blob) => Ok(vec![blob_input(blob)?]),
        Record::RemoteSource(source) => {
            let mut inputs = Vec::new();
            for uri in &source.inputs {
                for path in
                    bytemass::expand_inputs(lake, std::slice::from_ref(uri), &source.env).await?
                {
                    inputs.push(Input {
                        uri: path,
                        env: source.env.clone(),
                        size_bytes: None,
                    });
                }
            }
            Ok(inputs)
        }
        Record::Bytemass(_) => Err("a bytemass stream goes to `pqbench viz`".into()),
    }
}

/// A table-file's read: its blob's location, the lease's env, and the size the
/// log logged for the cross-check after the footer is read.
fn file_input(file: v1::TableFile) -> Result<Input, CliError> {
    let blob = file
        .blob
        .as_deref()
        .ok_or("a table-file record needs a blob to read")?;
    Ok(Input {
        uri: blob_uri(blob)?,
        env: file
            .credentials
            .as_ref()
            .map(v1::Credentials::to_env)
            .unwrap_or_default(),
        size_bytes: file.size_bytes,
    })
}

/// A blob's read: its location, and the size it claims.
fn blob_input(blob: v1::Blob) -> Result<Input, CliError> {
    Ok(Input {
        uri: blob_uri(&blob.name)?,
        env: BTreeMap::new(),
        size_bytes: blob.size_bytes,
    })
}

/// The storage location a blob name reads.
fn blob_uri(blob: &str) -> Result<String, CliError> {
    v1::name::blob_uri(blob).ok_or_else(|| CliError::from(format!("not a blob name: {blob}")))
}

/// Read one file's footer, cross-check the claimed size, and yield its blob
/// and column rows.
async fn measure(lake: &pqbench::Lake, input: Input) -> Result<Vec<Out>, CliError> {
    let measured = bytemass::measure_file(lake, &input.uri, &input.env)
        .await
        .map_err(|error| context(&input.uri, error))?;
    if let Some(size) = input.size_bytes.filter(|size| *size != 0) {
        if measured.blob.size_bytes != Some(size) {
            return Err(format!(
                "active file size differs from log: {} (expected {size}, found {:?})",
                input.uri, measured.blob.size_bytes
            )
            .into());
        }
    }
    Ok(measured_rows(measured))
}

/// One output record: a file's blob, then one row per column.
#[derive(Serialize)]
#[serde(untagged)]
enum Out {
    Blob(v1::Blob),
    Column(v1::Bytemass),
}

/// The records one measured file writes, in order.
fn measured_rows(measured: bytemass::MeasuredFile) -> Vec<Out> {
    let mut rows = Vec::with_capacity(1 + measured.columns.len());
    rows.push(Out::Blob(measured.blob));
    rows.extend(measured.columns.into_iter().map(Out::Column));
    rows
}
