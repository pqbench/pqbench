use std::collections::{BTreeMap, BTreeSet};
use std::io::IsTerminal;
use std::path::PathBuf;

use clap::Args;
use pqbench::bytemass;
use pqbench::table::TableFile;
use serde::Serialize;

use crate::document::{self, Record};
use crate::emit::{self, Emitter};
use crate::CliError;

/// Arguments for `bytemass`.
#[derive(Args)]
pub(crate) struct BytemassArgs {
    /// parquet paths, a `pqbench.table` document, or `-` for standard input
    inputs: Vec<String>,
    /// write the zstd NDJSON stream (required on a terminal)
    #[arg(short = 'o', long = "output", value_name = "FILE")]
    output: Option<PathBuf>,
    /// stream NDJSON (same as a pipe; kept for scripts)
    #[arg(long = "json", conflicts_with = "d3")]
    json: bool,
    /// emit a self-contained d3 treemap HTML instead of the stream
    #[arg(long = "d3")]
    d3: bool,
}

/// Build the typed request, measure, and stream each row as it is ready.
pub(crate) async fn run(args: &BytemassArgs) -> Result<(), CliError> {
    let _ = args.json;
    if args.inputs.is_empty() {
        if std::io::stdin().is_terminal() {
            return Err("bytemass needs parquet files or a table document".into());
        }
        return measure_document("-", args).await;
    }
    if args.inputs.len() == 1 && document::is_document(&args.inputs[0]).await {
        return measure_document(&args.inputs[0], args).await;
    }
    measure(args.inputs.clone(), BTreeMap::new(), args).await
}

async fn measure_document(input: &str, args: &BytemassArgs) -> Result<(), CliError> {
    if args.d3 {
        return measure_document_page(input, args).await;
    }
    let mut emit = Emitter::open("bytemass", args.output.as_deref())?;
    let mut stats = MassStats::default();
    let mut envs: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    let mut open: BTreeSet<String> = BTreeSet::new();
    emit.write(&BeginRecord {
        kind: "pqbench.bytemass",
        version: 1,
        event: "begin",
    })?;
    document::visit_input(input, async |record| {
        match record {
            Record::RemoteSource(source) => {
                for uri in source.inputs {
                    measure_input(&mut emit, &mut stats, &uri, uri.clone(), source.env.clone())
                        .await?;
                }
            }
            Record::Table(info) => {
                for file in info.files {
                    measure_file(&mut emit, &mut stats, &info.uri, file, info.env.clone()).await?;
                }
            }
            Record::TableRef(table) => {
                envs.insert(table.id, table.env);
            }
            Record::Begin(begin) => {
                envs.insert(begin.id.clone(), begin.env);
                open.insert(begin.id);
            }
            Record::File { id, file } => {
                let env = envs.get(&id).cloned().unwrap_or_default();
                measure_file(&mut emit, &mut stats, &id, file, env).await?;
            }
            Record::Commit { .. } => {}
            Record::End { id } => {
                open.remove(&id);
            }
        }
        Ok(())
    })
    .await?;
    if !open.is_empty() {
        return Err("table stream ended without end".into());
    }
    finish_stream(emit, &stats, args.output.as_deref())
}

async fn measure_file(
    emit: &mut Emitter,
    stats: &mut MassStats,
    id: &str,
    file: TableFile,
    env: BTreeMap<String, String>,
) -> Result<(), CliError> {
    let rows = bytemass::bytemass(&bytemass::BytemassRequest {
        inputs: vec![file.uri.clone()],
        env,
    })
    .await?;
    for row in &rows {
        if file.size_bytes != 0 && row.size_bytes != file.size_bytes {
            return Err(format!(
                "active file size differs from log: {} (expected {}, found {})",
                file.path, file.size_bytes, row.size_bytes
            )
            .into());
        }
        write_row(emit, id, row, stats)?;
    }
    Ok(())
}

async fn measure_input(
    emit: &mut Emitter,
    stats: &mut MassStats,
    id: &str,
    uri: String,
    env: BTreeMap<String, String>,
) -> Result<(), CliError> {
    measure_file(emit, stats, id, TableFile::new(uri.clone(), uri, 0), env).await
}

async fn measure_document_page(input: &str, args: &BytemassArgs) -> Result<(), CliError> {
    let mut rows = Vec::new();
    let mut env = BTreeMap::new();
    let mut begun = false;
    let mut ended = false;
    let mut remote = None;
    let mut oneshot = None;
    document::visit_input(input, async |record| {
        if remote.is_some() || oneshot.is_some() {
            return Err("document contains more than one value".into());
        }
        match record {
            Record::RemoteSource(source) => remote = Some(source),
            Record::Table(info) => oneshot = Some(info),
            Record::TableRef(_) => {}
            Record::Begin(begin) => {
                env = begin.env;
                begun = true;
            }
            Record::File { file, .. } => {
                rows.push(file.uri);
            }
            Record::Commit { .. } => {}
            Record::End { .. } => ended = true,
        }
        Ok(())
    })
    .await?;
    let inputs = if let Some(source) = remote {
        source.inputs
    } else if let Some(info) = oneshot {
        info.files.into_iter().map(|file| file.uri).collect()
    } else if !begun || !ended {
        return Err("table stream ended without end".into());
    } else {
        rows
    };
    let measured = bytemass::bytemass(&bytemass::BytemassRequest { inputs, env }).await?;
    write_page(&measured, args).await
}

async fn measure(
    inputs: Vec<String>,
    env: BTreeMap<String, String>,
    args: &BytemassArgs,
) -> Result<(), CliError> {
    if args.d3 {
        let rows = bytemass::bytemass(&bytemass::BytemassRequest { inputs, env }).await?;
        return write_page(&rows, args).await;
    }
    let mut emit = Emitter::open("bytemass", args.output.as_deref())?;
    let mut stats = MassStats::default();
    emit.write(&BeginRecord {
        kind: "pqbench.bytemass",
        version: 1,
        event: "begin",
    })?;
    for input in inputs {
        measure_input(&mut emit, &mut stats, &input, input.clone(), env.clone()).await?;
    }
    finish_stream(emit, &stats, args.output.as_deref())
}

fn write_row(
    emit: &mut Emitter,
    id: &str,
    row: &bytemass::MassRow,
    stats: &mut MassStats,
) -> Result<(), CliError> {
    stats.add(row);
    emit.write(&RowRecord {
        kind: "pqbench.bytemass-row",
        id,
        row,
    })
}

fn finish_stream(
    mut emit: Emitter,
    stats: &MassStats,
    output: Option<&std::path::Path>,
) -> Result<(), CliError> {
    emit.write(&EndRecord {
        kind: "pqbench.bytemass",
        event: "end",
        file_count: stats.file_rows.len(),
        num_rows: stats.num_rows(),
        column_count: stats.column_count,
    })?;
    emit.finish(&stats.summary(output))
}

async fn write_page(rows: &[bytemass::MassRow], args: &BytemassArgs) -> Result<(), CliError> {
    let html = bytemass::render_html(rows)?;
    let tty = std::io::stdout().is_terminal();
    if tty && args.output.is_none() {
        return Err("bytemass --d3 on a terminal needs -o <file>".into());
    }
    if let Some(path) = &args.output {
        tokio::fs::write(path, &html).await?;
    }
    if tty {
        let mut summary = format!("d3: {} column(s)\n", rows.len());
        if let Some(path) = &args.output {
            summary.push_str(&format!("output: {}\n", path.display()));
        }
        return emit::write_stdout(&summary);
    }
    emit::write_stdout(&html)
}

#[derive(Default)]
struct MassStats {
    file_rows: BTreeMap<String, u64>,
    column_count: usize,
}

impl MassStats {
    fn add(&mut self, row: &bytemass::MassRow) {
        self.file_rows.insert(row.uri.clone(), row.num_rows);
        self.column_count += 1;
    }

    fn num_rows(&self) -> u64 {
        self.file_rows.values().sum()
    }

    fn summary(&self, output: Option<&std::path::Path>) -> String {
        let mut out = format!(
            "files: {}\nrows: {}\ncolumns: {}\n",
            self.file_rows.len(),
            self.num_rows(),
            self.column_count
        );
        if let Some(path) = output {
            out.push_str(&format!("output: {}\n", path.display()));
        }
        out
    }
}

#[derive(Serialize)]
struct BeginRecord {
    kind: &'static str,
    version: u32,
    event: &'static str,
}

#[derive(Serialize)]
struct RowRecord<'a> {
    kind: &'static str,
    id: &'a str,
    #[serde(flatten)]
    row: &'a bytemass::MassRow,
}

#[derive(Serialize)]
struct EndRecord {
    kind: &'static str,
    event: &'static str,
    file_count: usize,
    num_rows: u64,
    column_count: usize,
}
