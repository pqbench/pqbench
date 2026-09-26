use std::io::IsTerminal;
use std::path::{Path, PathBuf};

use clap::Args;
use pqbench::bytemass::MassRow;
use pqbench::viz::{self, FileMass, MassRecord};

use crate::document::{self, Record};
use crate::CliError;

/// Arguments for `viz`.
#[derive(Args)]
pub(crate) struct VizArgs {
    /// bytemass NDJSON stream, or `-` for standard input
    input: Option<String>,
    /// write `PREFIX.html` (required)
    #[arg(short = 'o', long = "output", value_name = "PREFIX")]
    output: Option<PathBuf>,
}

pub(crate) async fn run(args: &VizArgs) -> Result<(), CliError> {
    let input = match &args.input {
        None if !std::io::stdin().is_terminal() => "-",
        None => return Err("viz needs a bytemass stream on standard input".into()),
        Some(value) => value.as_str(),
    };
    let prefix = match &args.output {
        Some(path) => prefix(path),
        None => return Err("viz writes an html page; pass --output".into()),
    };
    let (rows, files) = collect(input).await?;
    viz::write_report(&prefix, &rows, &files)?;
    if std::io::stdout().is_terminal() {
        print!("{}", summary(&prefix, &rows, &files));
    }
    Ok(())
}

async fn collect(input: &str) -> Result<(Vec<MassRecord>, Vec<FileMass>), CliError> {
    let mut rows = Vec::new();
    let mut files = Vec::new();
    let mut begun = false;
    document::visit_input(input, async |record| {
        match record {
            Record::BytemassBegin => begun = true,
            Record::BytemassFile(file) => files.push(file_mass(file)),
            Record::BytemassRow { id, row } => rows.push(mass_record(id, row)),
            Record::BytemassEnd => {}
            Record::Table(_)
            | Record::TableRef(_)
            | Record::Begin(_)
            | Record::Commit { .. }
            | Record::File { .. }
            | Record::End { .. }
            | Record::Lake(_)
            | Record::LakeSource(_)
            | Record::LakeBegin
            | Record::LakeEnd
            | Record::RemoteSource(_) => {
                return Err(
                    "viz reads a pqbench.bytemass stream; measure with `pqbench bytemass` first"
                        .into(),
                );
            }
        }
        Ok(())
    })
    .await?;
    if !begun {
        return Err(
            "viz reads a pqbench.bytemass stream; measure with `pqbench bytemass` first".into(),
        );
    }
    if rows.is_empty() {
        return Err("bytemass stream has no rows".into());
    }
    Ok((rows, files))
}

fn file_mass(file: document::BytemassFile) -> FileMass {
    FileMass {
        id: file.id,
        path: file.path,
        file: file.file,
        size: file.size,
        num_records: file.stats.as_ref().map(|stats| stats.num_records),
        bytes_per_row: file.stats.as_ref().and_then(|stats| stats.bytes_per_row),
        storage_class: file.storage_class,
        partition: hive_partition(&file.partition_values),
    }
}

fn hive_partition(values: &std::collections::BTreeMap<String, Option<String>>) -> String {
    values
        .iter()
        .map(|(key, value)| format!("{key}={}", value.as_deref().unwrap_or("null")))
        .collect::<Vec<_>>()
        .join("/")
}

fn mass_record(id: String, row: MassRow) -> MassRecord {
    MassRecord {
        id,
        file: row.uri,
        size: row.size_bytes,
        row_count: row.row_count,
        column: row.column,
        compressed_bytes: row.compressed_bytes,
        uncompressed_bytes: row.uncompressed_bytes,
        codec: row.codec,
        encodings: row.encodings.join(","),
        num_values: row.num_values,
        dictionary: row.dictionary,
        null_count: row.null_count,
        distinct_count: row.distinct_count,
        physical_type: row.physical_type,
        row_group: row.row_group,
        row_group_rows: row.row_group_rows,
        compressed_bytes_per_row: row.compressed_bytes_per_row,
        page_count: row.page_count,
    }
}

fn prefix(path: &Path) -> PathBuf {
    match path.extension().and_then(|ext| ext.to_str()) {
        Some("html" | "htm") => path.with_extension(""),
        _ => path.to_path_buf(),
    }
}

fn summary(prefix: &Path, rows: &[MassRecord], files: &[FileMass]) -> String {
    let measured = rows
        .iter()
        .map(|row| row.file.as_str())
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    let files = if files.is_empty() {
        measured
    } else {
        files.len()
    };
    format!(
        "files: {files}\ncolumns: {}\noutput: {}\n",
        rows.len(),
        prefix.with_extension("html").display()
    )
}
