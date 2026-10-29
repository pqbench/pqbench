//! Output: one NDJSON line per record on standard output.
//!
//! A record is serialized straight into the buffered writer, so a line costs
//! the writer's buffer rather than a copy of itself. The buffer reaches the
//! pipe when it fills and when the command finishes, and the write blocks
//! there — which is where a slow consumer slows the producer. Writes complete before the exit status is decided, so a
//! partial run's output is never truncated by its own error.

use std::io::{BufWriter, Write};

use serde::Serialize;

use crate::CliError;

/// One command's NDJSON stream.
pub(crate) struct Emitter {
    pipe: BufWriter<std::io::Stdout>,
    closed: bool,
}

impl Emitter {
    /// Open standard output.
    pub(crate) fn open() -> Self {
        Self {
            pipe: BufWriter::new(std::io::stdout()),
            closed: false,
        }
    }

    /// Write one record. A closed reader ends the stream, not the command.
    pub(crate) fn write(&mut self, value: &impl Serialize) -> Result<(), CliError> {
        if self.closed {
            return Ok(());
        }
        match self.write_line(value) {
            Ok(()) => Ok(()),
            Err(error) => {
                if closed_pipe(&error) {
                    self.closed = true;
                    Ok(())
                } else {
                    Err(error)
                }
            }
        }
    }

    /// One record's line, written as it serializes.
    fn write_line(&mut self, value: &impl Serialize) -> Result<(), CliError> {
        serde_json::to_writer(&mut self.pipe, value)
            .map_err(|error| format!("cannot serialize output: {error}"))?;
        self.pipe.write_all(b"\n")?;
        Ok(())
    }

    /// Flush what is buffered.
    pub(crate) fn finish(mut self) -> Result<(), CliError> {
        // A reader that has gone away has nothing left to flush.
        if let Err(error) = self.pipe.flush() {
            let error: CliError = error.into();
            if !closed_pipe(&error) {
                return Err(error);
            }
        }
        Ok(())
    }
}

fn closed_pipe(error: &CliError) -> bool {
    error
        .downcast_ref::<std::io::Error>()
        .is_some_and(|error| error.kind() == std::io::ErrorKind::BrokenPipe)
}
