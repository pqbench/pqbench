//! The CLI's error channel: rendering a cause chain, and deciding which
//! per-record failures a walk level carries on without.
//!
//! A level fans out one request per ref, so a single timeout would otherwise
//! end the whole walk: the `?` in the consumer drops the stream, the process
//! exits non-zero, and `pipefail` fails the pipeline. [`tolerate`] is the
//! per-record boundary instead — the shape `credentials check` uses for a table
//! it cannot read outside compute: report the ref and the cause on standard
//! error, drop the record, and let the rest of the stream continue.

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

/// Whether a per-record failure is one the walk carries on without: the
/// request or the storage read got no answer — a timeout, a dropped
/// connection, a store fault. An endpoint answer (a 429/4xx/5xx) or a
/// malformed document is not transient, so it still fails the level and the
/// shell's retry at a lower rate still sees it (decision 0007).
pub(crate) fn is_transient(error: &CliError) -> bool {
    if let Some(error) = error.downcast_ref::<pqbench::http::Error>() {
        return transport(error);
    }
    if let Some(error) = error.downcast_ref::<pqbench::catalog::list::Error>() {
        return matches!(error, pqbench::catalog::list::Error::Http(error) if transport(error));
    }
    if let Some(error) = error.downcast_ref::<pqbench::schema::list::Error>() {
        return matches!(error, pqbench::schema::list::Error::Http(error) if transport(error));
    }
    if let Some(error) = error.downcast_ref::<pqbench::table::list::Error>() {
        return matches!(error, pqbench::table::list::Error::Http(error) if transport(error));
    }
    if let Some(error) = error.downcast_ref::<pqbench::table::get::Error>() {
        return match error {
            pqbench::table::get::Error::Http(error) => transport(error),
            pqbench::table::get::Error::Metadata { source, .. } => table(source),
            _ => false,
        };
    }
    if let Some(error) = error.downcast_ref::<pqbench::credentials::get::Error>() {
        return matches!(error, pqbench::credentials::get::Error::Http(error) if transport(error));
    }
    if let Some(error) = error.downcast_ref::<pqbench::partition::list::Error>() {
        return match error {
            pqbench::partition::list::Error::Table(error) => table(error),
            _ => false,
        };
    }
    if let Some(error) = error.downcast_ref::<pqbench::file::list::Error>() {
        return match error {
            pqbench::file::list::Error::Table(error) => table(error),
            _ => false,
        };
    }
    if let Some(error) = error.downcast_ref::<pqbench::table::Error>() {
        return table(error);
    }
    if let Some(error) = error.downcast_ref::<pqbench::bytemass::Error>() {
        return matches!(error, pqbench::bytemass::Error::Storage(_));
    }
    false
}

/// A transport failure: the request never got an answer. A status answer is
/// not transient.
fn transport(error: &pqbench::http::Error) -> bool {
    matches!(error, pqbench::http::Error::Transport(_))
}

/// A storage failure: the object store faulted after its own retries, so the
/// object could not be read. A malformed or unrecognized table is permanent.
fn table(error: &pqbench::table::Error) -> bool {
    matches!(error, pqbench::table::Error::Storage(_))
}

/// The record's read: `Some(value)`, or `None` when the failure was transient
/// and has been reported on standard error. A real failure propagates, so the
/// level still fails on an answer it cannot use.
pub(crate) fn tolerate<T>(
    reference: &str,
    result: Result<T, CliError>,
) -> Result<Option<T>, CliError> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(error) if is_transient(&error) => {
            eprintln!("error: {reference}: {}", error_chain(error.as_ref()));
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

/// The one line a level prints after dropping records; nothing when it dropped
/// none (Rule of Silence).
pub(crate) fn report_dropped(what: &str, dropped: usize) {
    if dropped > 0 {
        eprintln!("{dropped} {what}(s) could not be read; the walk carried on without them");
    }
}
