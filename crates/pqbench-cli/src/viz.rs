use std::io::{IsTerminal, Read};
use std::path::{Path, PathBuf};

use clap::Args;
use pqbench::bytemass::MassRow;
use pqbench::viz::{self, MassRecord};

use crate::document::{self, Record};
use crate::CliError;

/// Arguments for `viz`.
#[derive(Args)]
pub(crate) struct VizArgs {
    /// bytemass NDJSON stream, or `-` for standard input
    input: Option<String>,
    /// write `PREFIX.sqlite` and `PREFIX.html` (required)
    #[arg(short = 'o', long = "output", value_name = "PREFIX")]
    output: Option<PathBuf>,
}

pub(crate) fn run(args: &VizArgs) -> Result<(), CliError> {
    let input = match &args.input {
        None if !std::io::stdin().is_terminal() => "-",
        None => return Err("viz needs a bytemass stream on standard input".into()),
        Some(value) => value.as_str(),
    };
    let prefix = match &args.output {
        Some(path) => prefix(path),
        None => return Err("viz writes sqlite and html; pass --output".into()),
    };
    let reader: Box<dyn Read> = if input == "-" {
        Box::new(std::io::stdin())
    } else {
        document::open_file(Path::new(input))?
    };
    let rows = collect(reader)?;
    viz::write_report(&prefix, &rows)?;
    if std::io::stdout().is_terminal() {
        print!("{}", summary(&prefix, &rows));
    }
    Ok(())
}

fn collect(reader: impl Read) -> Result<Vec<MassRecord>, CliError> {
    let mut rows = Vec::new();
    let mut begun = false;
    document::visit_records(reader, |record| {
        match record {
            Record::BytemassBegin => begun = true,
            Record::BytemassRow { id, row } => rows.push(mass_record(id, row)),
            Record::BytemassEnd => {}
            Record::Table(_)
            | Record::TableRef(_)
            | Record::Begin(_)
            | Record::Log { .. }
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
    })?;
    if !begun {
        return Err(
            "viz reads a pqbench.bytemass stream; measure with `pqbench bytemass` first".into(),
        );
    }
    if rows.is_empty() {
        return Err("bytemass stream has no rows".into());
    }
    Ok(rows)
}

fn mass_record(id: String, row: MassRow) -> MassRecord {
    MassRecord {
        id,
        file: row.file,
        size: row.size,
        num_rows: row.num_rows,
        column: row.column,
        compressed_bytes: row.compressed_bytes,
        uncompressed_bytes: row.uncompressed_bytes,
        codec: row.codec,
    }
}

fn prefix(path: &Path) -> PathBuf {
    match path.extension().and_then(|ext| ext.to_str()) {
        Some("html" | "htm" | "sqlite" | "db") => path.with_extension(""),
        _ => path.to_path_buf(),
    }
}

fn summary(prefix: &Path, rows: &[MassRecord]) -> String {
    let files = rows
        .iter()
        .map(|row| row.file.as_str())
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    format!(
        "files: {files}\ncolumns: {}\noutput: {}\noutput: {}\n",
        rows.len(),
        prefix.with_extension("sqlite").display(),
        prefix.with_extension("html").display()
    )
}
