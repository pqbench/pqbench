//! `bytemass`: read a Parquet footer and emit per-column byte masses.

use std::collections::BTreeMap;

use clap::Args;
use futures_util::stream::{self, StreamExt, TryStreamExt};
use pqbench::bytemass;
use pqbench::v1;
use serde::Serialize;

use crate::document::{self, Record};
use crate::emit::Emitter;
use crate::error::{context, fail_when_dropped};
use crate::source::{drain, stage};
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

/// Build the typed request, measure, and stream each record as it is ready.
pub(crate) async fn run(lake: &pqbench::Lake, args: &BytemassArgs) -> Result<(), CliError> {
    if args.inputs.is_empty() {
        if !is_stream() {
            return Err(
                "bytemass needs parquet files or a table-file / remote-source document".into(),
            );
        }
        return measure_document(lake, "-", args).await;
    }
    if args.inputs.len() == 1 && document::is_document(&args.inputs[0]).await {
        return measure_document(lake, &args.inputs[0], args).await;
    }
    measure(lake, args.inputs.clone(), BTreeMap::new(), args).await
}

async fn measure_document(
    lake: &pqbench::Lake,
    input: &str,
    args: &BytemassArgs,
) -> Result<(), CliError> {
    let mut emit = Emitter::open();
    let records = document::records(input).await?;
    let files = records
        .map(|record| async move { measure_record(lake, record).await })
        .buffer_unordered(args.fan_out.max(1))
        .map_ok(stream::iter)
        .try_flatten_unordered(Some(args.fan_out.max(1)))
        .flat_map(|file| {
            stream::iter(match file {
                Ok(file) => measured_rows(file).into_iter().map(Ok).collect(),
                Err(error) => vec![Err(error)],
            })
        });
    let dropped = drain(files, &mut emit).await?;
    emit.finish().await?;
    fail_when_dropped(dropped)
}

async fn measure(
    lake: &pqbench::Lake,
    inputs: Vec<String>,
    env: BTreeMap<String, String>,
    args: &BytemassArgs,
) -> Result<(), CliError> {
    let mut emit = Emitter::open();
    let inputs = bytemass::expand_inputs(lake, &inputs, &env).await?;
    let env = &env;
    let dropped = stage(
        stream::iter(inputs.into_iter().map(Ok)),
        args.fan_out,
        &mut emit,
        |input: String| async move {
            let measured = read_file(lake, &input, env)
                .await
                .map_err(|error| context(&input, error))?;
            Ok(measured_rows(measured))
        },
    )
    .await?;
    emit.finish().await?;
    fail_when_dropped(dropped)
}

/// Measure every file one document record names: a table-file is one file; a
/// remote source expands its inputs first. A read that fails is returned as an
/// `Err`; the caller reports it and carries on.
async fn measure_record(
    lake: &pqbench::Lake,
    record: Result<Record, CliError>,
) -> Result<Vec<Result<bytemass::MeasuredFile, CliError>>, CliError> {
    match record? {
        Record::File(file) => Ok(vec![read_table_file(lake, file).await?]),
        Record::RemoteSource(source) => measure_source(lake, source).await,
        Record::Blob(_) | Record::Bytemass(_) => {
            Err("a bytemass stream goes to `pqbench viz`".into())
        }
    }
}

/// Measure every file a remote source names. Expanding the source is a contract
/// step — a bad source fails the level; only each file's read is dropped.
async fn measure_source(
    lake: &pqbench::Lake,
    source: document::RemoteSource,
) -> Result<Vec<Result<bytemass::MeasuredFile, CliError>>, CliError> {
    let mut measured = Vec::new();
    for uri in source.inputs {
        let inputs = std::slice::from_ref(&uri);
        for input in bytemass::expand_inputs(lake, inputs, &source.env).await? {
            let read = read_file(lake, &input, &source.env)
                .await
                .map_err(|error| context(&input, error));
            measured.push(read);
        }
    }
    Ok(measured)
}

/// Read one file's footer. This is the I/O the fanout overlaps; [`emit_measured`]
/// writes each result as it finishes.
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

/// Read a table file's blob, and cross-check the log's size against the
/// footer's. A read that fails is returned as an `Err`; a name or size the
/// record gets wrong still fails the command.
async fn read_table_file(
    lake: &pqbench::Lake,
    file: v1::TableFile,
) -> Result<Result<bytemass::MeasuredFile, CliError>, CliError> {
    let blob = file
        .blob
        .as_deref()
        .ok_or("a table-file record needs a blob to read")?;
    let uri = v1::name::blob_uri(blob)
        .ok_or_else(|| CliError::from(format!("not a blob name: {blob}")))?;
    let env = file
        .credentials
        .as_ref()
        .map(v1::Credentials::to_env)
        .unwrap_or_default();
    let read = read_file(lake, &uri, &env)
        .await
        .map_err(|error| context(&uri, error));
    let Ok(measured) = read else {
        return Ok(read);
    };
    if let Some(size) = file.size_bytes {
        if size != 0 && measured.blob.size_bytes != Some(size) {
            return Err(format!(
                "active file size differs from log: {uri} (expected {size}, found {:?})",
                measured.blob.size_bytes
            )
            .into());
        }
    }
    Ok(Ok(measured))
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
