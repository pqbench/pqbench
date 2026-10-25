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
use crate::stdin::is_stream;
use crate::CliError;

/// Arguments for `bytemass`.
#[derive(Args)]
pub(crate) struct BytemassArgs {
    /// parquet paths, a table-file / remote-source document, or `-` for
    /// standard input
    inputs: Vec<String>,
    /// files in flight at once
    #[arg(long, default_value_t = 128)]
    fan_out: usize,
}

/// Read the files the arguments name and stream each record as it is ready.
pub(crate) async fn run(lake: &pqbench::Lake, args: &BytemassArgs) -> Result<(), CliError> {
    let mut emit = Emitter::open();
    let dropped = stage(
        inputs(lake, args).await?,
        args.fan_out,
        &mut emit,
        |input: Input| async move { read(lake, input).await },
    )
    .await?;
    emit.finish().await?;
    fail_when_dropped(dropped)
}

/// One file to read. The arguments and a document's records both lead here, so
/// there is one fan-out and one place a file can fail.
struct Input {
    /// The file's location.
    uri: String,
    /// The storage environment to read it with.
    env: BTreeMap<String, String>,
    /// The size the log logged, when a record carried one.
    size_bytes: Option<u64>,
}

/// The files the arguments name: parquet paths and globs expand here; `-` or a
/// document path is the records it carries.
async fn inputs<'a>(
    lake: &'a pqbench::Lake,
    args: &BytemassArgs,
) -> Result<BoxStream<'a, Result<Input, CliError>>, CliError> {
    if args.inputs.is_empty() {
        if !is_stream() {
            return Err(
                "bytemass needs parquet files or a table-file / remote-source document".into(),
            );
        }
        return Ok(document_inputs(
            lake,
            document::records("-").await?,
            args.fan_out,
        ));
    }
    if args.inputs.len() == 1 && document::is_document(&args.inputs[0]).await {
        return Ok(document_inputs(
            lake,
            document::records(&args.inputs[0]).await?,
            args.fan_out,
        ));
    }
    let paths = bytemass::expand_inputs(lake, &args.inputs, &BTreeMap::new()).await?;
    Ok(stream::iter(
        paths
            .into_iter()
            .map(|uri| {
                Ok(Input {
                    uri,
                    env: BTreeMap::new(),
                    size_bytes: None,
                })
            })
            .collect::<Vec<_>>(),
    )
    .boxed())
}

/// A document's records as files: each record expands with `fan_out` of them in
/// flight, and a record that cannot be expanded becomes an item of its own, so
/// the stage reports it and carries on.
fn document_inputs<'a>(
    lake: &'a pqbench::Lake,
    records: BoxStream<'static, Result<Record, CliError>>,
    fan_out: usize,
) -> BoxStream<'a, Result<Input, CliError>> {
    records
        .map(move |record| async move { record_inputs(lake, record?).await })
        .buffer_unordered(fan_out.max(1))
        .flat_map(|inputs| match inputs {
            Ok(inputs) => stream::iter(inputs).boxed(),
            Err(error) => stream::iter([Err(error)]).boxed(),
        })
        .boxed()
}

/// The files one document record names: a table-file is its blob and lease; a
/// remote source is its inputs, expanded with the source's env.
async fn record_inputs(
    lake: &pqbench::Lake,
    record: Record,
) -> Result<Vec<Result<Input, CliError>>, CliError> {
    match record {
        Record::File(file) => Ok(vec![table_file_input(file)]),
        Record::RemoteSource(source) => {
            let mut inputs = Vec::new();
            for uri in &source.inputs {
                for input in
                    bytemass::expand_inputs(lake, std::slice::from_ref(uri), &source.env).await?
                {
                    inputs.push(Ok(Input {
                        uri: input,
                        env: source.env.clone(),
                        size_bytes: None,
                    }));
                }
            }
            Ok(inputs)
        }
        Record::Blob(_) | Record::Bytemass(_) => {
            Err("a bytemass stream goes to `pqbench viz`".into())
        }
    }
}

/// A table-file's read: its blob's location, the lease's env, and the size the
/// log logged for the cross-check after the footer is read.
fn table_file_input(file: v1::TableFile) -> Result<Input, CliError> {
    let blob = file
        .blob
        .as_deref()
        .ok_or("a table-file record needs a blob to read")?;
    let uri = v1::name::blob_uri(blob)
        .ok_or_else(|| CliError::from(format!("not a blob name: {blob}")))?;
    Ok(Input {
        uri,
        env: file
            .credentials
            .as_ref()
            .map(v1::Credentials::to_env)
            .unwrap_or_default(),
        size_bytes: file.size_bytes,
    })
}

/// Read one file's footer, cross-check the log's size when the record carried
/// one, and yield its blob and column rows.
async fn read(lake: &pqbench::Lake, input: Input) -> Result<Vec<Out>, CliError> {
    let measured = read_file(lake, &input.uri, &input.env)
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

/// Read one file's footer. This is the I/O the fan-out overlaps; the level
/// writes each result, and its rows, as the file finishes.
async fn read_file(
    lake: &pqbench::Lake,
    uri: &str,
    env: &BTreeMap<String, String>,
) -> Result<bytemass::MeasuredFile, CliError> {
    bytemass::measure_files(
        lake,
        &bytemass::BytemassRequest {
            inputs: vec![uri.to_string()],
            env: env.clone(),
        },
    )
    .await?
    .pop()
    .ok_or_else(|| CliError::from("bytemass read no file"))
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
