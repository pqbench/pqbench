//! Shared command output: an aligned table on a terminal, NDJSON to a pipe
//! and/or a zstd file.
//!
//! A terminal is interactive, so it gets columns a human can read; a pipe gets
//! one JSON value per line so the next command can start immediately. `Auto`
//! follows the stdout kind, so `pqbench table | pqbench bytemass` stays a
//! machine pipeline while `pqbench table` shows a table. `--format` (and the
//! older `--json`) override that choice. `-o` always receives the NDJSON
//! stream, independent of what stdout shows.

use std::fs::File;
use std::io::{IsTerminal, Write};
use std::path::Path;

use serde::Serialize;

use crate::CliError;

/// `--format` value; `Auto` follows the stdout kind.
#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum Format {
    /// table on a terminal, NDJSON on a pipe
    Auto,
    /// aligned columns for a human
    Table,
    /// NDJSON for the next command
    Json,
}

/// The format a command's stdout actually uses.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Resolved {
    Json,
    Table,
}

impl Format {
    /// `--format` wins, then `--json`, then the terminal test.
    pub(crate) fn resolve(self, json: bool) -> Resolved {
        match self {
            Format::Json => Resolved::Json,
            Format::Table => Resolved::Table,
            Format::Auto if json => Resolved::Json,
            Format::Auto if std::io::stdout().is_terminal() => Resolved::Table,
            Format::Auto => Resolved::Json,
        }
    }
}

/// How a table column pads its cells.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Align {
    Left,
    Right,
}

/// A data record that renders as a JSON line or a table row.
///
/// Structural records (`begin`/`end`) stay out of the table: they go through
/// [`Emitter::write_event`] and only reach the NDJSON sinks.
pub(crate) trait Row: Serialize {
    /// Column headers; rows with the same headers share one table block.
    const HEADER: &'static [&'static str];
    /// Per-column alignment, the same length as `HEADER`.
    const ALIGN: &'static [Align];
    /// One cell per column.
    fn cells(&self) -> Vec<String>;
}

/// Destination for one command's stream.
pub(crate) struct Emitter {
    pipe: bool,
    closed: bool,
    file: Option<zstd::Encoder<'static, File>>,
    table: Option<Table>,
}

impl Emitter {
    /// Open stdout in `format` and the optional `-o` zstd file.
    pub(crate) fn open(output: Option<&Path>, format: Resolved) -> Result<Self, CliError> {
        let file = match output {
            Some(path) => Some(open_zstd(path)?),
            None => None,
        };
        let table = match format {
            Resolved::Table => Some(Table::new()),
            Resolved::Json => None,
        };
        Ok(Self {
            pipe: format == Resolved::Json,
            closed: false,
            file,
            table,
        })
    }

    /// Write a structural record: NDJSON sinks only.
    pub(crate) fn write_event(&mut self, value: &impl Serialize) -> Result<(), CliError> {
        self.write_json(value)
    }

    /// Write a data row: table cells on a terminal, NDJSON otherwise.
    pub(crate) fn write_row<T: Row>(&mut self, row: &T) -> Result<(), CliError> {
        if let Some(table) = &mut self.table {
            table.push(row);
        }
        self.write_json(row)
    }

    fn write_json(&mut self, value: &impl Serialize) -> Result<(), CliError> {
        if self.pipe && !self.closed {
            match write_record(&mut std::io::stdout(), value) {
                Ok(()) => {}
                Err(error) if closed_pipe(&error) => self.closed = true,
                Err(error) => return Err(error),
            }
        }
        if let Some(file) = &mut self.file {
            write_record(file, value)?;
        }
        Ok(())
    }

    /// Finish the zstd frame. On a table, render it followed by `summary`.
    pub(crate) fn finish(mut self, summary: &str) -> Result<(), CliError> {
        if let Some(encoder) = self.file.take() {
            encoder
                .finish()
                .map_err(|error| format!("cannot finish output: {error}"))?;
        }
        if let Some(table) = self.table.take() {
            write_stdout(&table.render(summary))?;
        }
        Ok(())
    }
}

/// Write plain text to stdout. A closed reader is not an error.
pub(crate) fn write_stdout(text: &str) -> Result<(), CliError> {
    match std::io::stdout().write_all(text.as_bytes()) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::BrokenPipe => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn closed_pipe(error: &CliError) -> bool {
    error
        .downcast_ref::<std::io::Error>()
        .is_some_and(|error| error.kind() == std::io::ErrorKind::BrokenPipe)
}

fn open_zstd(path: &Path) -> Result<zstd::Encoder<'static, File>, CliError> {
    let file =
        File::create(path).map_err(|error| format!("cannot write {}: {error}", path.display()))?;
    Ok(zstd::Encoder::new(file, 0)
        .map_err(|error| format!("cannot compress {}: {error}", path.display()))?)
}

fn write_record(writer: &mut impl Write, value: &impl Serialize) -> Result<(), CliError> {
    let line = serde_json::to_string(value)
        .map_err(|error| format!("cannot serialize output: {error}"))?;
    writeln!(writer, "{line}")?;
    writer.flush()?;
    Ok(())
}

/// One block of rows sharing a header.
struct Section {
    header: &'static [&'static str],
    align: &'static [Align],
    rows: Vec<Vec<String>>,
}

/// Bounded, aligned columns for a terminal.
///
/// The buffer stops after `limit` rows so a large stream cannot flood the
/// terminal; the rest are counted and reported as hidden.
struct Table {
    sections: Vec<Section>,
    seen: usize,
}

impl Table {
    const LIMIT: usize = 1000;

    fn new() -> Self {
        Self {
            sections: Vec::new(),
            seen: 0,
        }
    }

    fn push<T: Row>(&mut self, row: &T) {
        self.seen += 1;
        if self.buffered() >= Self::LIMIT {
            return;
        }
        let index = match self.sections.last() {
            Some(section) if std::ptr::eq(section.header, T::HEADER) => self.sections.len() - 1,
            _ => {
                self.sections.push(Section {
                    header: T::HEADER,
                    align: T::ALIGN,
                    rows: Vec::new(),
                });
                self.sections.len() - 1
            }
        };
        self.sections[index].rows.push(row.cells());
    }

    fn buffered(&self) -> usize {
        self.sections.iter().map(|section| section.rows.len()).sum()
    }

    fn render(&self, summary: &str) -> String {
        let mut out = String::new();
        for section in &self.sections {
            if !out.is_empty() {
                out.push('\n');
            }
            out.push_str(&render_section(section));
        }
        if self.seen > self.buffered() {
            out.push_str(&format!(
                "\n… {} more row(s); pipe or --format json to see all\n",
                self.seen - self.buffered()
            ));
        }
        out.push_str(summary);
        if !out.ends_with('\n') {
            out.push('\n');
        }
        out
    }
}

fn render_section(section: &Section) -> String {
    let mut widths: Vec<usize> = section
        .header
        .iter()
        .map(|text| text.chars().count())
        .collect();
    for row in &section.rows {
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.chars().count());
        }
    }
    let mut out = String::new();
    out.push_str(&format_row(
        section.header.iter().copied(),
        &widths,
        section.align,
    ));
    let rule: Vec<String> = widths.iter().map(|width| "-".repeat(*width)).collect();
    out.push_str(&format_row(
        rule.iter().map(String::as_str),
        &widths,
        section.align,
    ));
    for row in &section.rows {
        out.push_str(&format_row(
            row.iter().map(String::as_str),
            &widths,
            section.align,
        ));
    }
    out
}

fn format_row<'a>(
    cells: impl Iterator<Item = &'a str>,
    widths: &[usize],
    align: &[Align],
) -> String {
    let mut line = String::new();
    for (index, (cell, width)) in cells.zip(widths).enumerate() {
        if index > 0 {
            line.push_str("  ");
        }
        let pad = width.saturating_sub(cell.chars().count());
        match align.get(index).copied().unwrap_or(Align::Left) {
            Align::Right => {
                line.push_str(&" ".repeat(pad));
                line.push_str(cell);
            }
            Align::Left => {
                line.push_str(cell);
                line.push_str(&" ".repeat(pad));
            }
        }
    }
    while line.ends_with(' ') {
        line.pop();
    }
    line.push('\n');
    line
}
