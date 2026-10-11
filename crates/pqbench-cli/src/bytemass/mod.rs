//! `bytemass`: read a Parquet footer and emit per-column byte masses.

use std::collections::BTreeMap;
use std::io::IsTerminal;
use std::path::PathBuf;

use clap::Args;
use futures_util::stream::{self, StreamExt};
use pqbench::bytemass;
use pqbench::v1;
use serde::Serialize;

use crate::document::{self, Record};
use crate::emit::{Align, Emitter, Format, Row};
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
    /// also load ColumnIndex/OffsetIndex (one extra range per file)
    #[arg(long)]
    indexes: bool,
    /// Scan page headers without requiring page indexes (extra reads).
    #[arg(long)]
    pages: bool,
    /// files in flight at once
    #[arg(long, default_value_t = 128)]
    fan_out: usize,
}

/// Build the typed request, measure, and stream each row as it is ready.
pub(crate) async fn run(lake: &pqbench::Lake, args: &BytemassArgs) -> Result<(), CliError> {
    if args.inputs.is_empty() {
        if std::io::stdin().is_terminal() {
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
    let options = (args.indexes, args.pages);
    let records = document::records(input).await?;
    let mut reads = records
        .map(|record| measure_record(lake, record, options))
        .buffer_unordered(args.fan_out.max(1));
    while let Some(batch) = reads.next().await {
        for measured in batch? {
            emit_measured(&mut emit, &mut stats, measured).await?;
        }
    }
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
    let options = (args.indexes, args.pages);
    let mut reads = stream::iter(bytemass::expand_inputs(lake, &inputs, &env).await?)
        .map(|input| {
            let env = env.clone();
            async move {
                let file = v1::TableFile {
                    name: v1::name::table_file_of(&v1::name::local_table(&input), &input),
                    blob: Some(v1::name::blob_of(&input)),
                    ..v1::TableFile::default()
                };
                measure_one(lake, file, env, options).await
            }
        })
        .buffer_unordered(args.fan_out.max(1));
    while let Some(measured) = reads.next().await {
        emit_measured(&mut emit, &mut stats, measured?).await?;
    }
    finish_stream(emit, &stats, args.output.as_deref()).await
}

/// One file's footer read, ready to emit: the log's facts joined onto the
/// measured file, plus the page headers when `--pages` asked for them.
struct Measured {
    id: String,
    measured: bytemass::MeasuredFile,
    pages: Vec<bytemass::PageRecord>,
}

/// Measure every file one document record names: a table-file is one file; a
/// remote-source expands its inputs first.
async fn measure_record(
    lake: &pqbench::Lake,
    record: Result<Record, CliError>,
    options: (bool, bool),
) -> Result<Vec<Measured>, CliError> {
    match record? {
        Record::File(file) => {
            let env = file
                .credentials
                .as_ref()
                .map(v1::Credentials::to_env)
                .unwrap_or_default();
            Ok(vec![measure_one(lake, file, env, options).await?])
        }
        Record::RemoteSource(source) => {
            let mut measured = Vec::new();
            for uri in source.inputs {
                let inputs = std::slice::from_ref(&uri);
                for input in bytemass::expand_inputs(lake, inputs, &source.env).await? {
                    let file = v1::TableFile {
                        name: v1::name::table_file_of(&v1::name::local_table(&input), &input),
                        blob: Some(v1::name::blob_of(&input)),
                        ..v1::TableFile::default()
                    };
                    measured.push(measure_one(lake, file, source.env.clone(), options).await?);
                }
            }
            Ok(measured)
        }
        Record::BytemassFile(_) | Record::BytemassRow { .. } | Record::BytemassPage => {
            Err("a bytemass stream goes to `pqbench viz`".into())
        }
    }
}

/// Read one file's footer and join the log's facts onto it. This is the I/O the
/// fanout overlaps; [`emit_measured`] writes each result as it finishes.
async fn measure_one(
    lake: &pqbench::Lake,
    file: v1::TableFile,
    env: BTreeMap<String, String>,
    options: (bool, bool),
) -> Result<Measured, CliError> {
    let blob = file
        .blob
        .as_deref()
        .ok_or("a table-file record needs a blob to read")?;
    let uri = v1::name::blob_uri(blob)
        .ok_or_else(|| CliError::from(format!("not a blob name: {blob}")))?;
    let mut measured = bytemass::measure_files(
        lake,
        &bytemass::BytemassRequest {
            inputs: vec![uri.clone()],
            env: env.clone(),
            indexes: options.0,
        },
    )
    .await?
    .pop()
    .ok_or("bytemass read no file")?;
    if let Some(size) = file.size_bytes {
        if size != 0 && measured.file.size != size {
            return Err(format!(
                "active file size differs from log: {uri} (expected {size}, found {})",
                measured.file.size
            )
            .into());
        }
    }
    let (table, path) = v1::name::parse_table_file(&file.name).unzip();
    let id = table.unwrap_or_else(|| uri.clone());
    measured.file.id = id.clone();
    if let Some(path) = path {
        measured.file.path = path;
    }
    measured.file.partition_values = file.partition_values.clone();
    let pages = if options.1 {
        bytemass::scan_pages(lake, &measured.file.file, &env).await?
    } else {
        Vec::new()
    };
    Ok(Measured {
        id,
        measured,
        pages,
    })
}

/// Emit one measured file: its file record, then its page headers and rows.
async fn emit_measured(
    emit: &mut Emitter,
    stats: &mut MassStats,
    measured: Measured,
) -> Result<(), CliError> {
    let Measured {
        id,
        measured,
        pages,
    } = measured;
    stats
        .file_rows
        .insert(measured.file.file.clone(), measured.row_count);
    emit.write_event(&FileRecord {
        kind: "pqbench.bytemass-file",
        file: &measured.file,
    })
    .await?;
    for page in &pages {
        emit.write_row(&PageRow {
            kind: "pqbench.bytemass-page",
            id: &id,
            page,
        })
        .await?;
    }
    for row in &measured.columns {
        write_row(emit, &id, row, stats).await?;
    }
    Ok(())
}

async fn write_row(
    emit: &mut Emitter,
    id: &str,
    row: &bytemass::MassRow,
    stats: &mut MassStats,
) -> Result<(), CliError> {
    stats.add(row);
    emit.write_row(&RowRecord {
        kind: "pqbench.bytemass-row",
        id,
        row,
    })
    .await
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
    file_rows: BTreeMap<String, u64>,
    column_count: usize,
}

impl MassStats {
    fn add(&mut self, row: &bytemass::MassRow) {
        self.file_rows.insert(row.uri.clone(), row.row_count);
        self.column_count += 1;
    }

    fn row_count(&self) -> u64 {
        self.file_rows.values().sum()
    }

    fn summary(&self, output: Option<&std::path::Path>) -> String {
        let mut out = format!(
            "files: {}\nrows: {}\ncolumns: {}\n",
            self.file_rows.len(),
            self.row_count(),
            self.column_count
        );
        if let Some(path) = output {
            out.push_str(&format!("output: {}\n", path.display()));
        }
        out
    }
}

#[derive(Serialize)]
struct FileRecord<'a> {
    kind: &'static str,
    #[serde(flatten)]
    file: &'a bytemass::FileStat,
}

#[derive(Serialize)]
struct RowRecord<'a> {
    kind: &'static str,
    id: &'a str,
    #[serde(flatten)]
    row: &'a bytemass::MassRow,
}

impl Row for RowRecord<'_> {
    const HEADER: &'static [&'static str] =
        &["column", "type", "codec", "encodings", "bytes", "values"];
    const ALIGN: &'static [Align] = &[
        Align::Left,
        Align::Left,
        Align::Left,
        Align::Left,
        Align::Right,
        Align::Right,
    ];

    fn cells(&self) -> Vec<String> {
        let row = self.row;
        vec![
            row.column.clone(),
            row.physical_type.clone(),
            row.codec.clone(),
            row.encodings.join(","),
            row.compressed_bytes.to_string(),
            row.num_values.to_string(),
        ]
    }
}

#[derive(Serialize)]
struct PageRow<'a> {
    kind: &'static str,
    id: &'a str,
    #[serde(flatten)]
    page: &'a bytemass::PageRecord,
}
impl Row for PageRow<'_> {
    const HEADER: &'static [&'static str] = &[
        "column",
        "row_group",
        "page",
        "type",
        "encoding",
        "compressed_bytes",
        "values",
    ];
    const ALIGN: &'static [Align] = &[
        Align::Left,
        Align::Right,
        Align::Right,
        Align::Left,
        Align::Left,
        Align::Right,
        Align::Right,
    ];
    fn cells(&self) -> Vec<String> {
        let p = self.page;
        vec![
            p.column.clone(),
            p.row_group.to_string(),
            p.page.to_string(),
            p.header.page_type.clone(),
            p.header.encoding.clone().unwrap_or_default(),
            p.header.compressed_bytes.to_string(),
            p.header.value_count.map_or("-".into(), |n| n.to_string()),
        ]
    }
}
