//! `bytemass`: read a Parquet footer and emit per-column byte masses.

use std::collections::BTreeMap;
use std::path::PathBuf;

use clap::Args;
use futures_util::stream::{self, StreamExt};
use pqbench::bytemass;
use pqbench::v1;
use serde::Serialize;

use crate::document::{self, Record};
use crate::emit::{Align, Emitter, Format, Row};
use crate::error::{report_dropped, tolerate};
use crate::stdin::is_stream;
use crate::CliError;

/// Arguments for `bytemass`.
#[derive(Args)]
pub(crate) struct BytemassArgs {
    /// parquet paths, a table-file / remote-source document, or `-` for
    /// standard input
    inputs: Vec<String>,
    /// also write the lz4 NDJSON stream to FILE
    #[arg(short = 'o', long = "output", value_name = "FILE")]
    output: Option<PathBuf>,
    /// stream NDJSON (same as --format json; kept for scripts)
    #[arg(long = "json")]
    json: bool,
    /// stdout format: auto (table on a terminal) | table | json
    #[arg(long, value_enum, default_value_t = Format::Auto)]
    format: Format,
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
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(args.json))?;
    let mut stats = MassStats::default();
    let mut dropped = 0;
    let records = document::records(input).await?;
    let mut reads = records
        .map(|record| measure_record(lake, record))
        .buffer_unordered(args.fan_out.max(1));
    while let Some(batch) = reads.next().await {
        let Some(batch) = batch? else {
            dropped += 1;
            continue;
        };
        for measured in batch {
            emit_measured(&mut emit, &mut stats, measured).await?;
        }
    }
    report_dropped("file", dropped);
    finish_stream(emit, &stats, args.output.as_deref()).await
}

async fn measure(
    lake: &pqbench::Lake,
    inputs: Vec<String>,
    env: BTreeMap<String, String>,
    args: &BytemassArgs,
) -> Result<(), CliError> {
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(args.json))?;
    let mut stats = MassStats::default();
    let mut dropped = 0;
    let mut reads = stream::iter(bytemass::expand_inputs(lake, &inputs, &env).await?)
        .map(|input| {
            let env = env.clone();
            async move {
                let read = read_file(lake, &input, &env).await;
                tolerate(&input, read)
            }
        })
        .buffer_unordered(args.fan_out.max(1));
    while let Some(measured) = reads.next().await {
        let Some(measured) = measured? else {
            dropped += 1;
            continue;
        };
        emit_measured(&mut emit, &mut stats, measured).await?;
    }
    report_dropped("file", dropped);
    finish_stream(emit, &stats, args.output.as_deref()).await
}

/// Measure every file one document record names: a table-file is one file; a
/// remote-source expands its inputs first. A file that cannot be read is
/// reported and dropped, so one timeout does not end the walk.
async fn measure_record(
    lake: &pqbench::Lake,
    record: Result<Record, CliError>,
) -> Result<Option<Vec<bytemass::MeasuredFile>>, CliError> {
    match record? {
        Record::File(file) => {
            let reference = file
                .blob
                .clone()
                .unwrap_or_else(|| "a table file".to_string());
            let read = read_table_file(lake, file)
                .await
                .map(|measured| vec![measured]);
            tolerate(&reference, read)
        }
        Record::RemoteSource(source) => {
            let reference = source.inputs.join(", ");
            tolerate(&reference, measure_source(lake, source).await)
        }
        Record::Blob(_) | Record::Bytemass(_) => {
            Err("a bytemass stream goes to `pqbench viz`".into())
        }
    }
}

/// Measure every file a remote source names.
async fn measure_source(
    lake: &pqbench::Lake,
    source: document::RemoteSource,
) -> Result<Vec<bytemass::MeasuredFile>, CliError> {
    let mut measured = Vec::new();
    for uri in source.inputs {
        let inputs = std::slice::from_ref(&uri);
        for input in bytemass::expand_inputs(lake, inputs, &source.env).await? {
            measured.push(read_file(lake, &input, &source.env).await?);
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
/// footer's.
async fn read_table_file(
    lake: &pqbench::Lake,
    file: v1::TableFile,
) -> Result<bytemass::MeasuredFile, CliError> {
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
    let measured = read_file(lake, &uri, &env).await?;
    if let Some(size) = file.size_bytes {
        if size != 0 && measured.blob.size_bytes != Some(size) {
            return Err(format!(
                "active file size differs from log: {uri} (expected {size}, found {:?})",
                measured.blob.size_bytes
            )
            .into());
        }
    }
    Ok(measured)
}

/// Emit one measured file: its blob, then one row per column.
async fn emit_measured(
    emit: &mut Emitter,
    stats: &mut MassStats,
    measured: bytemass::MeasuredFile,
) -> Result<(), CliError> {
    stats.files += 1;
    emit.write_event(&measured.blob).await?;
    for column in &measured.columns {
        stats.columns += 1;
        emit.write_row(&ColumnRow { column }).await?;
    }
    Ok(())
}

async fn finish_stream(
    emit: Emitter,
    stats: &MassStats,
    output: Option<&std::path::Path>,
) -> Result<(), CliError> {
    emit.finish(&stats.summary(output)).await
}

#[derive(Default)]
struct MassStats {
    files: usize,
    columns: usize,
}

impl MassStats {
    fn summary(&self, output: Option<&std::path::Path>) -> String {
        let mut out = format!("files: {}\ncolumns: {}\n", self.files, self.columns);
        if let Some(path) = output {
            out.push_str(&format!("output: {}\n", path.display()));
        }
        out
    }
}

/// A column as a table row: the column path and the facts a human reads.
#[derive(Serialize)]
struct ColumnRow<'a> {
    #[serde(flatten)]
    column: &'a v1::Bytemass,
}

impl Row for ColumnRow<'_> {
    const HEADER: &'static [&'static str] = &["column", "codec", "encodings", "bytes", "values"];
    const ALIGN: &'static [Align] = &[
        Align::Left,
        Align::Left,
        Align::Left,
        Align::Right,
        Align::Right,
    ];

    fn cells(&self) -> Vec<String> {
        let column = self.column;
        let name = v1::name::parse_bytemass(&column.name)
            .map(|(_, column)| column)
            .unwrap_or_else(|| column.name.clone());
        vec![
            name,
            column.codec.clone().unwrap_or_default(),
            column.encodings.join(","),
            column.compressed_bytes.unwrap_or(0).to_string(),
            column.value_count.unwrap_or(0).to_string(),
        ]
    }
}
