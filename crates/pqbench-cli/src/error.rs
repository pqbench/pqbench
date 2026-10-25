//! The CLI's error channel: rendering a cause chain, and the per-record
//! boundary that keeps one failed read from ending a walk.
//!
//! A level fans out one request per ref, so a single failure would otherwise
//! end the whole walk: the `?` in the consumer drops the stream, the process
//! exits non-zero, and `pipefail` fails the pipeline. [`tolerate`] is the
//! boundary instead — report the ref and the cause on standard error, drop the
//! record, keep the stream going.
//!
//! It is deliberately type-agnostic. The CLI decides what a failed record
//! read means; it does not read the core's error taxonomy, so a new core error
//! variant cannot silently change the policy.

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

/// The record's read: `Some(value)`, or `None` when it failed and has been
/// reported on standard error. Every per-record failure is dropped; the level
/// carries on and summarises what it lost.
pub(crate) fn tolerate<T>(reference: &str, result: Result<T, CliError>) -> Option<T> {
    match result {
        Ok(value) => Some(value),
        Err(error) => {
            eprintln!("error: {reference}: {}", error_chain(error.as_ref()));
            None
        }
    }
}

/// The level's end: after the output is finished, report a partial run as a
/// failure. The dropped records were already named on standard error; this
/// carries the count into the exit status, so the run is green only when
/// nothing was lost.
pub(crate) fn fail_when_dropped(what: &str, dropped: usize) -> Result<(), CliError> {
    if dropped == 0 {
        return Ok(());
    }
    Err(format!("{dropped} {what}(s) could not be read").into())
}
