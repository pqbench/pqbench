//! The CLI's error channel: a failed record read carries its reference and its
//! source chain to the level, which reports it and carries on; the level's end
//! turns the count into exit 1 after the output is finished.
//!
//! The read only wraps: it does not decide. The level's loop decides whether a
//! record's read failed (report, count, continue) or the input/output failed
//! (propagate with `?`). The wrapper keeps the error itself, so the rendered
//! message has the same cause chain as a top-level `?` — no error is reduced to
//! a string at the point of failure.

use crate::CliError;

/// Render an error with its whole source chain. `Display` alone drops the
/// cause: a transport failure reads only "request failed", while the
/// socket-level error (reset, exhaustion, DNS) sits one `source()` deeper.
pub(crate) fn error_chain(error: &dyn std::error::Error) -> String {
    let mut rendered = error.to_string();
    let mut source = error.source();
    while let Some(error) = source {
        rendered.push_str("\n  caused by: ");
        rendered.push_str(&error.to_string());
        source = error.source();
    }
    rendered
}

/// A failed per-record read: the record's reference and the error, with its
/// source chain intact. The read attaches the reference; the level reports it
/// and counts it. `what` is the record kind for the level's summary.
pub(crate) struct RecordError {
    what: &'static str,
    reference: String,
    error: CliError,
}

impl RecordError {
    pub(crate) fn new(
        what: &'static str,
        reference: impl Into<String>,
        error: impl Into<CliError>,
    ) -> Self {
        Self {
            what,
            reference: reference.into(),
            error: error.into(),
        }
    }

    pub(crate) fn what(&self) -> &'static str {
        self.what
    }

    pub(crate) fn report(&self) {
        eprintln!(
            "error: {}: {}",
            self.reference,
            error_chain(self.error.as_ref())
        );
    }
}

/// One record's read: the record, or the failure with its reference.
pub(crate) enum ReadOutcome<T> {
    Record(T),
    Dropped(RecordError),
}

/// The level's end: after the output is finished, report a partial run as a
/// failure. The dropped records were already named on standard error; this
/// carries the count into the exit status, so the run is green only when
/// nothing was lost.
pub(crate) fn fail_when_dropped(counts: &[(&str, usize)]) -> Result<(), CliError> {
    let lost: Vec<String> = counts
        .iter()
        .filter(|(_, count)| *count > 0)
        .map(|(what, count)| format!("{count} {what}(s)"))
        .collect();
    if lost.is_empty() {
        return Ok(());
    }
    Err(format!("{} could not be read", lost.join(" and ")).into())
}
