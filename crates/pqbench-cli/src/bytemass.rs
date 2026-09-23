use std::collections::{BTreeMap, BTreeSet};
use std::io::{IsTerminal, Read};
use std::num::NonZeroUsize;
use std::path::PathBuf;

use clap::Args;
use pqbench::bytemass;
use pqbench::table::TableFile;
use serde::Serialize;

use tokio::sync::mpsc;
use tokio::task::JoinSet;

use crate::document::{self, Record};
use crate::emit::Emit;
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
    /// files to measure at once
    #[arg(long, default_value = "4", value_name = "N")]
    concurrency: NonZeroUsize,
}

/// Build the typed request, measure, and stream each row as it is ready.
pub(crate) fn run(args: &BytemassArgs) -> Result<(), CliError> {
    let _ = args.json;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(run_async(args))
}

async fn run_async(args: &BytemassArgs) -> Result<(), CliError> {
    if args.inputs.is_empty() {
        if std::io::stdin().is_terminal() {
            return Err("bytemass needs parquet files or a table document".into());
        }
        return measure_document("-", args).await;
    }
    if args.inputs.len() == 1 && document::looks_like_document(&args.inputs[0]) {
        return measure_document(&args.inputs[0], args).await;
    }
    measure(args.inputs.clone(), BTreeMap::new(), args).await
}

async fn measure_document(input: &str, args: &BytemassArgs) -> Result<(), CliError> {
    if args.d3 {
        let reader: Box<dyn Read> = if input == "-" {
            Box::new(std::io::stdin())
        } else {
            document::open_file(std::path::Path::new(input))?
        };
        return measure_document_page(reader, args).await;
    }
    let (tx, mut rx) = mpsc::unbounded_channel::<Result<Record, String>>();
    let path = input.to_string();
    std::thread::spawn(move || {
        let reader: Box<dyn Read> = if path == "-" {
            Box::new(std::io::stdin())
        } else {
            match document::open_file(std::path::Path::new(&path)) {
                Ok(reader) => reader,
                Err(error) => {
                    let _ = tx.send(Err(error.to_string()));
                    return;
                }
            }
        };
        if let Err(error) = document::visit_records(reader, |record| {
            tx.send(Ok(record))
                .map_err(|_| "bytemass input closed".into())
        }) {
            let _ = tx.send(Err(error.to_string()));
        }
    });

    let mut emit = Emit::open("bytemass", args.output.as_deref())?;
    let mut stats = MassStats::default();
    let mut envs: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    let mut open: BTreeSet<String> = BTreeSet::new();
    let mut set: JoinSet<Result<Measured, String>> = JoinSet::new();
    let concurrency = args.concurrency.get();
    emit.write(&BeginRecord {
        kind: "pqbench.bytemass",
        version: 1,
        event: "begin",
    })?;
    loop {
        tokio::select! {
            done = set.join_next(), if !set.is_empty() => {
                if let Some(done) = done {
                    emit_measured(&mut emit, done, &mut stats)?;
                }
            }
            rec = rx.recv() => {
                match rec {
                    None => break,
                    Some(Err(error)) => return Err(error.into()),
                    Some(Ok(record)) => {
                        queue_record(
                            record,
                            &mut set,
                            &mut emit,
                            &mut stats,
                            &mut envs,
                            &mut open,
                            concurrency,
                        )
                        .await?;
                    }
                }
            }
        }
    }
    while let Some(done) = set.join_next().await {
        emit_measured(&mut emit, done, &mut stats)?;
    }
    if !open.is_empty() {
        return Err("table stream ended without end".into());
    }
    finish_stream(emit, &stats, args.output.as_deref())
}

struct Measured {
    id: String,
    file: TableFile,
    rows: Vec<bytemass::MassRow>,
}

#[allow(clippy::too_many_arguments)]
async fn queue_record(
    record: Record,
    set: &mut JoinSet<Result<Measured, String>>,
    emit: &mut Emit,
    stats: &mut MassStats,
    envs: &mut BTreeMap<String, BTreeMap<String, String>>,
    open: &mut BTreeSet<String>,
    concurrency: usize,
) -> Result<(), CliError> {
    match record {
        Record::RemoteSource(source) => {
            for uri in source.inputs {
                spawn_input(
                    set,
                    emit,
                    stats,
                    concurrency,
                    uri.clone(),
                    uri,
                    source.env.clone(),
                )
                .await?;
            }
        }
        Record::Table(info) => {
            for file in info.files {
                spawn_file(
                    set,
                    emit,
                    stats,
                    concurrency,
                    info.uri.clone(),
                    file,
                    info.env.clone(),
                )
                .await?;
            }
        }
        Record::TableRef(_) | Record::Lake(_) | Record::LakeSource(_) => {
            return Err(
                "bytemass measures files after `pqbench table` loads them; pass a lake to `pqbench table` first"
                    .into(),
            );
        }
        Record::LakeBegin | Record::LakeEnd => {}
        Record::Begin(begin) => {
            envs.insert(begin.id.clone(), begin.env);
            open.insert(begin.id);
        }
        Record::File { id, file } => {
            let env = envs.get(&id).cloned().unwrap_or_default();
            spawn_file(set, emit, stats, concurrency, id, file, env).await?;
        }
        Record::Log { .. } => {}
        Record::End { id } => {
            open.remove(&id);
        }
    }
    Ok(())
}

async fn spawn_file(
    set: &mut JoinSet<Result<Measured, String>>,
    emit: &mut Emit,
    stats: &mut MassStats,
    concurrency: usize,
    id: String,
    file: TableFile,
    env: BTreeMap<String, String>,
) -> Result<(), CliError> {
    while set.len() >= concurrency {
        if let Some(done) = set.join_next().await {
            emit_measured(emit, done, stats)?;
        }
    }
    set.spawn(async move {
        let rows = bytemass::bytemass(&bytemass::BytemassRequest {
            inputs: vec![file.uri.clone()],
            env,
        })
        .await
        .map_err(|error| error.to_string())?;
        for row in &rows {
            if file.size != 0 && row.size != file.size {
                return Err(format!(
                    "active file size differs from log: {} (expected {}, found {})",
                    file.path, file.size, row.size
                ));
            }
        }
        Ok(Measured { id, file, rows })
    });
    Ok(())
}

async fn spawn_input(
    set: &mut JoinSet<Result<Measured, String>>,
    emit: &mut Emit,
    stats: &mut MassStats,
    concurrency: usize,
    id: String,
    uri: String,
    env: BTreeMap<String, String>,
) -> Result<(), CliError> {
    spawn_file(
        set,
        emit,
        stats,
        concurrency,
        id,
        TableFile::new(uri.clone(), uri, 0),
        env,
    )
    .await
}

fn emit_measured(
    emit: &mut Emit,
    done: Result<Result<Measured, String>, tokio::task::JoinError>,
    stats: &mut MassStats,
) -> Result<(), CliError> {
    let measured = done.map_err(|error| error.to_string())??;
    for row in &measured.rows {
        if measured.file.size != 0 && row.size != measured.file.size {
            return Err(format!(
                "active file size differs from log: {} (expected {}, found {})",
                measured.file.path, measured.file.size, row.size
            )
            .into());
        }
        write_row(emit, &measured.id, row, stats)?;
    }
    Ok(())
}

async fn measure_document_page(reader: impl Read, args: &BytemassArgs) -> Result<(), CliError> {
    let mut rows = Vec::new();
    let mut env = BTreeMap::new();
    let mut begun = false;
    let mut ended = false;
    let mut remote = None;
    let mut oneshot = None;
    document::visit_records(reader, |record| {
        if remote.is_some() || oneshot.is_some() {
            return Err("document contains more than one value".into());
        }
        match record {
            Record::RemoteSource(source) => remote = Some(source),
            Record::Table(info) => oneshot = Some(info),
            Record::Begin(begin) => {
                env = begin.env;
                begun = true;
            }
            Record::File { file, .. } => {
                rows.push(file.uri);
            }
            Record::Log { .. } => {}
            Record::End { .. } => ended = true,
            Record::TableRef(_)
            | Record::Lake(_)
            | Record::LakeSource(_)
            | Record::LakeBegin
            | Record::LakeEnd => {
                return Err(
                    "bytemass measures files after `pqbench table` loads them; pass a lake to `pqbench table` first"
                        .into(),
                );
            }
        }
        Ok(())
    })?;
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
    write_page(&measured, args)
}

async fn measure(
    inputs: Vec<String>,
    env: BTreeMap<String, String>,
    args: &BytemassArgs,
) -> Result<(), CliError> {
    if args.d3 {
        let rows = bytemass::bytemass(&bytemass::BytemassRequest { inputs, env }).await?;
        return write_page(&rows, args);
    }
    let mut emit = Emit::open("bytemass", args.output.as_deref())?;
    let mut stats = MassStats::default();
    let mut set: JoinSet<Result<Measured, String>> = JoinSet::new();
    emit.write(&BeginRecord {
        kind: "pqbench.bytemass",
        version: 1,
        event: "begin",
    })?;
    for input in inputs {
        spawn_input(
            &mut set,
            &mut emit,
            &mut stats,
            args.concurrency.get(),
            input.clone(),
            input,
            env.clone(),
        )
        .await?;
    }
    while let Some(done) = set.join_next().await {
        emit_measured(&mut emit, done, &mut stats)?;
    }
    finish_stream(emit, &stats, args.output.as_deref())
}

fn write_row(
    emit: &mut Emit,
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
    mut emit: Emit,
    stats: &MassStats,
    output: Option<&std::path::Path>,
) -> Result<(), CliError> {
    emit.write(&EndRecord {
        kind: "pqbench.bytemass",
        event: "end",
        file_count: stats.files.len(),
        num_rows: stats.num_rows(),
        column_count: stats.columns,
    })?;
    emit.finish(&stats.summary(output))
}

fn write_page(rows: &[bytemass::MassRow], args: &BytemassArgs) -> Result<(), CliError> {
    let html = bytemass::render_html(rows)?;
    let tty = std::io::stdout().is_terminal();
    if tty && args.output.is_none() {
        return Err("bytemass --d3 on a terminal needs -o <file>".into());
    }
    if let Some(path) = &args.output {
        std::fs::write(path, &html)?;
    }
    if tty {
        let mut summary = format!("d3: {} column(s)\n", rows.len());
        if let Some(path) = &args.output {
            summary.push_str(&format!("output: {}\n", path.display()));
        }
        print!("{summary}");
        return Ok(());
    }
    print!("{html}");
    Ok(())
}

#[derive(Default)]
struct MassStats {
    files: BTreeSet<String>,
    file_rows: BTreeMap<String, u64>,
    columns: usize,
}

impl MassStats {
    fn add(&mut self, row: &bytemass::MassRow) {
        self.files.insert(row.file.clone());
        self.file_rows.insert(row.file.clone(), row.num_rows);
        self.columns += 1;
    }

    fn num_rows(&self) -> u64 {
        self.file_rows.values().sum()
    }

    fn summary(&self, output: Option<&std::path::Path>) -> String {
        let mut out = format!(
            "files: {}\nrows: {}\ncolumns: {}\n",
            self.files.len(),
            self.num_rows(),
            self.columns
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
