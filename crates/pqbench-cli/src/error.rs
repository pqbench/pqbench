//! The CLI's error channel: a failed record read carries its reference and its
//! source chain to the level, which reports it and carries on; the level's end
//! turns the count into exit 1 after the output is finished.
//!
//! A failed record read is an `Err` with `context`'s reference on it: the cause
//! chain survives, so the rendered message reads like a top-level `?` and no
//! error is reduced to a string at the point of failure. The combinator
//! reports the failure on standard error and counts it; the exit status is the
//! only aggregate.

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

/// A failure with the record it came from; the cause stays in the chain.
#[derive(Debug)]
struct Context {
    reference: String,
    cause: CliError,
}

impl std::fmt::Display for Context {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.reference)
    }
}

impl std::error::Error for Context {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.cause.as_ref())
    }
}

/// Attach a record's reference to a failure, keeping its cause chain.
pub(crate) fn context(reference: impl Into<String>, cause: impl Into<CliError>) -> CliError {
    Box::new(Context {
        reference: reference.into(),
        cause: cause.into(),
    })
}

/// The level's end: after the output is finished, a run that lost records is a
/// failure. The losses were already named on standard error; this only carries
/// the fact into the exit status, so the run is green when nothing was lost.
pub(crate) fn fail_when_dropped(dropped: usize) -> Result<(), CliError> {
    if dropped == 0 {
        return Ok(());
    }
    Err(Box::new(Partial))
}

/// A run that wrote its output but lost records. The losses are already on
/// standard error; the type exists only to set the exit status without
/// printing a second message.
#[derive(Debug)]
pub(crate) struct Partial;

impl std::fmt::Display for Partial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("partial run")
    }
}

impl std::error::Error for Partial {}

/// Report a failure on standard error, with its cause chain.
pub(crate) fn report(error: &CliError) {
    eprintln!("error: {}", error_chain(error.as_ref()));
}
