//! Output: one NDJSON line per record on standard output.
//!
//! A record is serialized into one line and awaited into tokio's buffered
//! stdout, so a write yields to the runtime rather than parking the thread the
//! walk runs on. A consumer that stops reading applies backpressure at the
//! await, and the requests already in flight behind it can still complete — or
//! time out — on the same runtime. One record is resident at a time, and the
//! buffer reaches the pipe when it fills and when the command finishes.
//!
//! Writes complete before the exit status is decided, so a partial run's output
//! is never truncated by its own error.

use std::ops::ControlFlow;

use serde::Serialize;
use tokio::io::AsyncWriteExt;

use crate::CliError;

/// One command's NDJSON stream.
pub(crate) struct Emitter {
    pipe: tokio::io::Stdout,
    closed: bool,
}

impl Emitter {
    /// Open standard output.
    pub(crate) fn open() -> Self {
        Self {
            pipe: tokio::io::stdout(),
            closed: false,
        }
    }

    /// Write one record. Returns `Break(())` when the reader has closed
    /// stdout so the caller can stop upstream work.
    pub(crate) async fn write(
        &mut self,
        value: &impl Serialize,
    ) -> Result<ControlFlow<()>, CliError> {
        if self.closed {
            return Ok(ControlFlow::Break(()));
        }
        match self.write_line(value).await {
            Ok(()) => Ok(ControlFlow::Continue(())),
            Err(error) => {
                if closed_pipe(&error) {
                    self.closed = true;
                    Ok(ControlFlow::Break(()))
                } else {
                    Err(error)
                }
            }
        }
    }

    /// One record's line, written once it is serialized.
    async fn write_line(&mut self, value: &impl Serialize) -> Result<(), CliError> {
        let mut line = serde_json::to_string(value)
            .map_err(|error| format!("cannot serialize output: {error}"))?;
        line.push('\n');
        self.pipe.write_all(line.as_bytes()).await?;
        Ok(())
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
