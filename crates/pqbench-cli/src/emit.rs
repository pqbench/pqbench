//! Shared command output: NDJSON to a pipe and/or a zstd file.
//!
//! A terminal prints only a short summary and requires `-o`. A pipe writes
//! each record as it is produced so the next command can start immediately.

use std::fs::File;
use std::io::{IsTerminal, Write};
use std::path::Path;

use serde::Serialize;

use crate::CliError;

/// Destination for one command's stream.
pub(crate) struct Emit {
    pipe: bool,
    file: Option<zstd::Encoder<'static, File>>,
}

impl Emit {
    /// Open stdout and optional `-o`. A terminal without `-o` is an error.
    pub(crate) fn open(command: &str, output: Option<&Path>) -> Result<Self, CliError> {
        let tty = std::io::stdout().is_terminal();
        if tty && output.is_none() {
            return Err(
                format!("{command} on a terminal needs -o <file> to write the stream").into(),
            );
        }
        let file = match output {
            Some(path) => Some(open_zstd(path)?),
            None => None,
        };
        Ok(Self { pipe: !tty, file })
    }

    /// Write one JSON value as a line and flush both sinks.
    pub(crate) fn write(&mut self, value: &impl Serialize) -> Result<(), CliError> {
        if self.pipe {
            write_record(&mut std::io::stdout(), value)?;
        }
        if let Some(file) = &mut self.file {
            write_record(file, value)?;
        }
        Ok(())
    }

    /// Finish the zstd frame. On a terminal, print `summary`.
    pub(crate) fn finish(mut self, summary: &str) -> Result<(), CliError> {
        if let Some(encoder) = self.file.take() {
            encoder
                .finish()
                .map_err(|error| format!("cannot finish output: {error}"))?;
        }
        if !self.pipe {
            print!("{summary}");
        }
        Ok(())
    }
}

fn open_zstd(path: &Path) -> Result<zstd::Encoder<'static, File>, CliError> {
    let file =
        File::create(path).map_err(|error| format!("cannot write {}: {error}", path.display()))?;
    Ok(zstd::Encoder::new(file, 0)
        .map_err(|error| format!("cannot compress {}: {error}", path.display()))?)
}

fn write_record(writer: &mut impl Write, value: &impl Serialize) -> Result<(), CliError> {
    serde_json::to_writer(&mut *writer, value)
        .map_err(|error| format!("cannot serialize output: {error}"))?;
    writeln!(writer)?;
    writer.flush()?;
    Ok(())
}
