//! The walk's input: the environment's config and the parent-level refs.
//!
//! Every level is a stateless filter: it reads only its parent's records
//! (`list schemas` reads `Catalog` names, one per line) and writes its own. The
//! endpoint, bearer, and dialect come from the resolved configuration —
//! `~/.pqbenchrc`, the environment, then the flags (`pqbench setup` prints it
//! for the shell to eval):
//!
//! ```text
//! PQB_ENDPOINT=… PQB_TOKEN=… pqbench list catalogs | pqbench list schemas | …
//! ```
//!
//! `tee` writes each level into the job tree; `get` stages emit the same
//! resource as the `list` they read from, so they enrich a stream in place.
//!
//! Records are read one at a time as the command consumes them, so a level
//! never buffers its parent: a slow consumer stops reading, and the pipe
//! backpressures the producer.

use futures_util::stream::{self, LocalBoxStream, Stream, StreamExt};
use futures_util::TryStreamExt;
use serde_json::Value;
use std::future::Future;
use tokio::io::{AsyncBufReadExt, BufReader, Lines};

use pqbench::v1::Resource;

use crate::stdin::is_stream;
use crate::CliError;

/// The parent records on standard input: whether standard input is a stream,
/// the peeked first record, and the rest of the lines.
pub(crate) struct Input {
    /// Whether standard input is a stream (a pipe or a redirected file), not a
    /// terminal or other descriptor a read would block on.
    pub piped: bool,
    /// The first record, when stdin carried one.
    pub first: Option<Value>,
    /// The lines after the first record.
    pub lines: Lines<BufReader<tokio::io::Stdin>>,
}

/// The parent records: the peeked first record, then the rest of stdin.
pub(crate) fn records(
    command: &'static str,
    first: Option<Value>,
    lines: Lines<BufReader<tokio::io::Stdin>>,
) -> impl Stream<Item = Result<Value, CliError>> + Unpin {
    let first = stream::iter(first).map(Ok::<_, CliError>);
    let rest = stream::try_unfold(lines, move |mut lines| async move {
        loop {
            let Some(line) = lines.next_line().await? else {
                return Ok(None);
            };
            if line.trim().is_empty() {
                continue;
            }
            let record: Value = serde_json::from_str(&line)
                .map_err(|error| format!("{command} reads NDJSON records: {error}"))?;
            return Ok(Some((record, lines)));
        }
    });
    Box::pin(first.chain(rest))
}

/// Read stdin's first record (a parent ref, kept for the command's stream),
/// without resolving config.
pub(crate) async fn read_input(command: &'static str) -> Result<Input, CliError> {
    let piped = is_stream();
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut first = None;
    if piped {
        while let Some(line) = lines.next_line().await? {
            if line.trim().is_empty() {
                continue;
            }
            let record: Value = serde_json::from_str(&line)
                .map_err(|error| format!("{command} reads NDJSON records: {error}"))?;
            first = Some(record);
            break;
        }
    }
    Ok(Input {
        piped,
        first,
        lines,
    })
}

/// A command's records: each record, or the failure that replaced it.
pub(crate) type Records<'a, B> = LocalBoxStream<'a, Result<B, CliError>>;

/// `map` with `fan_out` reads in flight: one record per parent.
pub(crate) fn map<'a, A, B, E, F, Fut>(
    parents: impl Stream<Item = Result<A, CliError>> + 'a,
    fan_out: usize,
    read: F,
) -> Records<'a, B>
where
    A: Resource + 'a,
    B: 'a,
    E: Into<CliError> + 'a,
    F: Fn(A) -> Fut + Clone + 'a,
    Fut: Future<Output = Result<B, E>> + 'a,
{
    parents
        .map(move |parent| {
            let read = read.clone();
            async move {
                let parent = parent?;
                let reference = parent.name().to_owned();
                read(parent)
                    .await
                    .map_err(move |cause| crate::error::context(&reference, cause))
            }
        })
        .buffer_unordered(fan_out.max(1))
        .boxed_local()
}

/// `filterMap` with `fan_out` reads in flight: a parent keeps one record or
/// none, and a dropped parent is not a failure.
pub(crate) fn filter_map<'a, A, B, E, F, Fut>(
    parents: impl Stream<Item = Result<A, CliError>> + 'a,
    fan_out: usize,
    read: F,
) -> Records<'a, B>
where
    A: Resource + 'a,
    B: 'a,
    E: Into<CliError> + 'a,
    F: Fn(A) -> Fut + Clone + 'a,
    Fut: Future<Output = Result<Option<B>, E>> + 'a,
{
    parents
        .map(move |parent| {
            let read = read.clone();
            async move {
                let parent = parent?;
                let reference = parent.name().to_owned();
                read(parent)
                    .await
                    .map_err(move |cause| crate::error::context(&reference, cause))
            }
        })
        .buffer_unordered(fan_out.max(1))
        .filter_map(|row| async move {
            match row {
                Ok(Some(row)) => Some(Ok(row)),
                Ok(None) => None,
                Err(error) => Some(Err(error)),
            }
        })
        .boxed_local()
}

/// `flatMap` with `fan_out` streams in flight: a parent becomes its stream.
///
/// `read` may fail before it yields anything (the table it loads, a window it
/// rejects); that failure is reported under the parent, like a stream item.
pub(crate) fn flat_map<'a, A, B, E, S, F, Fut>(
    parents: impl Stream<Item = Result<A, CliError>> + 'a,
    fan_out: usize,
    read: F,
) -> Records<'a, B>
where
    A: Resource + 'a,
    B: 'a,
    E: Into<CliError> + 'a,
    S: Stream<Item = Result<B, E>> + 'a,
    F: Fn(A) -> Fut + Clone + 'a,
    Fut: Future<Output = Result<S, CliError>> + 'a,
{
    parents
        .map(move |parent| {
            let read = read.clone();
            async move {
                let parent = match parent {
                    Ok(parent) => parent,
                    Err(error) => return stream::iter([Err(error)]).boxed_local(),
                };
                let reference = parent.name().to_owned();
                match read(parent).await {
                    Ok(stream) => stream
                        .map_err(move |cause| crate::error::context(&reference, cause))
                        .boxed_local(),
                    Err(cause) => {
                        let error = crate::error::context(&reference, cause);
                        stream::iter([Err(error)]).boxed_local()
                    }
                }
            }
        })
        .buffer_unordered(fan_out.max(1))
        .flatten_unordered(Some(fan_out.max(1)))
        .boxed_local()
}

/// The sink: write every record, name a failure on stderr, count it, and set
/// the exit status.
pub(crate) async fn drive<B: serde::Serialize>(rows: Records<'_, B>) -> Result<(), CliError> {
    let mut emit = crate::emit::Emitter::open();
    futures_util::pin_mut!(rows);
    let mut dropped = 0;
    while let Some(row) = rows.next().await {
        match row {
            Ok(row) => emit.write(&row).await?,
            Err(error) => {
                crate::error::report(&error);
                dropped += 1;
            }
        }
    }
    emit.finish().await?;
    crate::error::fail_when_dropped(dropped)
}
