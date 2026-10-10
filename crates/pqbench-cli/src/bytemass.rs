use std::collections::BTreeMap;
use std::io::IsTerminal;
use std::path::PathBuf;

use clap::Args;
use futures_util::StreamExt;
use pqbench::bytemass;
use pqbench::table::TableFile;
use serde::Serialize;

use crate::document::{self, Record};
use crate::emit::{Align, Emitter, Format, Row};
use crate::CliError;

/// Arguments for `bytemass`.
#[derive(Args)]
pub(crate) struct BytemassArgs {
    /// parquet paths, a `pqbench.table-file` / `pqbench.remote-source`
    /// document, or `-` for standard input
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
}

/// Build the typed request, measure, and stream each row as it is ready.
pub(crate) async fn run(args: &BytemassArgs) -> Result<(), CliError> {
    if args.inputs.is_empty() {
        if std::io::stdin().is_terminal() {
            return Err(
                "bytemass needs parquet files or a pqbench.table-file / pqbench.remote-source document"
                    .into(),
            );
        }
        return measure_document("-", args).await;
    }
    if args.inputs.len() == 1 && document::is_document(&args.inputs[0]).await {
        return measure_document(&args.inputs[0], args).await;
    }
    measure(args.inputs.clone(), BTreeMap::new(), args).await
}

async fn measure_document(input: &str, args: &BytemassArgs) -> Result<(), CliError> {
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(args.json))?;
    let mut stats = MassStats::default();
    let mut records = document::records(input).await?;
    while let Some(record) = records.next().await {
        match record? {
            Record::RemoteSource(source) => {
                for uri in source.inputs {
                    measure_input(
                        &mut emit,
                        &mut stats,
                        &uri,
                        uri.clone(),
                        source.env.clone(),
                        (args.indexes, args.pages),
                    )
                    .await?;
                }
            }
            Record::File { id, file, env } => {
                measure_file(
                    &mut emit,
                    &mut stats,
                    &id,
                    file,
                    env,
                    (args.indexes, args.pages),
                )
                .await?;
            }
            Record::BytemassFile(_) | Record::BytemassRow { .. } | Record::BytemassPage => {
                return Err("a bytemass stream goes to `pqbench viz`".into());
            }
        }
    }
    finish_stream(emit, &stats, args.output.as_deref()).await
}

async fn measure_file(
    emit: &mut Emitter,
    stats: &mut MassStats,
    id: &str,
    file: TableFile,
    env: BTreeMap<String, String>,
    options: (bool, bool),
) -> Result<(), CliError> {
    let measured = bytemass::measure_files(&bytemass::BytemassRequest {
        inputs: vec![file.uri.clone()],
        env: env.clone(),
        indexes: options.0,
    })
    .await?;
    for mut measured in measured {
        if file.size_bytes != 0 && measured.file.size != file.size_bytes {
            return Err(format!(
                "active file size differs from log: {} (expected {}, found {})",
                file.path, file.size_bytes, measured.file.size
            )
            .into());
        }
        measured.file.id = id.to_owned();
        if file.uri == measured.file.file {
            measured.file.path = file.path.clone();
        }
        measured.file.partition_values = file.partition_values.clone();
        measured.file.stats = file.stats.clone();
        stats
            .file_rows
            .insert(measured.file.file.clone(), measured.row_count);
        emit.write_event(&FileRecord {
            kind: "pqbench.bytemass-file",
            file: &measured.file,
        })
        .await?;
        if options.1 {
            for page in bytemass::scan_pages(&measured.file.file, &env).await? {
                emit.write_row(&PageRow {
                    kind: "pqbench.bytemass-page",
                    id,
                    page: &page,
                })
                .await?;
            }
        }
        for row in &measured.columns {
            write_row(emit, id, row, stats).await?;
        }
    }
    Ok(())
}

async fn measure_input(
    emit: &mut Emitter,
    stats: &mut MassStats,
    id: &str,
    uri: String,
    env: BTreeMap<String, String>,
    options: (bool, bool),
) -> Result<(), CliError> {
    for input in bytemass::expand_inputs(&[uri], &env).await? {
        measure_file(
            emit,
            stats,
            id,
            TableFile::new(input.clone(), input, 0),
            env.clone(),
            options,
        )
        .await?;
    }
    Ok(())
}

async fn measure(
    inputs: Vec<String>,
    env: BTreeMap<String, String>,
    args: &BytemassArgs,
) -> Result<(), CliError> {
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(args.json))?;
    let mut stats = MassStats::default();
    for input in bytemass::expand_inputs(&inputs, &env).await? {
        measure_input(
            &mut emit,
            &mut stats,
            &input,
            input.clone(),
            env.clone(),
            (args.indexes, args.pages),
        )
        .await?;
    }
    finish_stream(emit, &stats, args.output.as_deref()).await
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
