//! Output: one NDJSON line per record on standard output.
//!
//! Each record is serialized and written directly to tokio's stdout without a
//! user-space `BufWriter`, so a closed reader (`BrokenPipe`) surfaces
//! immediately on write and lets the caller cancel upstream work via
//! [`ControlFlow::Break`].
//!
//! Writes complete before the exit status is decided, so a partial run's output
//! is never truncated by its own error.

use std::io::ErrorKind;
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
        let mut line = serde_json::to_vec(value)
            .map_err(|error| format!("cannot serialize output: {error}"))?;
        line.push(b'\n');
        match self.pipe.write_all(&line).await {
            Ok(()) => Ok(ControlFlow::Continue(())),
            Err(error) if error.kind() == ErrorKind::BrokenPipe => {
                self.closed = true;
                Ok(ControlFlow::Break(()))
            }
            Err(error) => Err(error.into()),
        }
    }

    /// Flush standard output.
    pub(crate) async fn finish(mut self) -> Result<(), CliError> {
        // A reader that has gone away has nothing left to flush.
        match self.pipe.flush().await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == ErrorKind::BrokenPipe => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}
