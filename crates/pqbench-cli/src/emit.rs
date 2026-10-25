//! Output: one NDJSON line per record on standard output.
//!
//! A pipe write awaits the event loop, so the stream is buffered: a row costs a
//! copy into the buffer, and the buffer reaches the pipe when it fills and when
//! the command finishes — which is where a slow consumer slows the producer.
//! Writes complete before the exit status is decided, so a partial run's output
//! is never truncated by its own error.

use serde::Serialize;
use tokio::io::{AsyncWriteExt, BufWriter};

use crate::CliError;

/// One command's NDJSON stream.
pub(crate) struct Emitter {
    pipe: BufWriter<tokio::io::Stdout>,
    closed: bool,
}

impl Emitter {
    /// Open standard output.
    pub(crate) fn open() -> Self {
        Self {
            pipe: BufWriter::new(tokio::io::stdout()),
            closed: false,
        }
    }

    /// Write one record. A closed reader ends the stream, not the command.
    pub(crate) async fn write(&mut self, value: &impl Serialize) -> Result<(), CliError> {
        if self.closed {
            return Ok(());
        }
        let mut line = serde_json::to_string(value)
            .map_err(|error| format!("cannot serialize output: {error}"))?;
        line.push('\n');
        match self.pipe.write_all(line.as_bytes()).await {
            Ok(()) => Ok(()),
            Err(error) => {
                let error: CliError = error.into();
                if closed_pipe(&error) {
                    self.closed = true;
                    Ok(())
                } else {
                    Err(error)
                }
            }
        }
    }

    /// Flush what is buffered.
    pub(crate) async fn finish(mut self) -> Result<(), CliError> {
        // A reader that has gone away has nothing left to flush.
        if let Err(error) = self.pipe.flush().await {
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
