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

/// The dotted `catalog.schema.table` a table ref names, for a message; the ref
/// itself when the name is not a table ref (a path read names a local table).
pub(crate) fn dotted_name(name: &str) -> String {
    match pqbench::v1::name::parse_table(name) {
        Some((catalog, schema, table)) => format!("{catalog}.{schema}.{table}"),
        None => name.to_string(),
    }
}

/// Write every record a level's parent expands to, reporting what fails and
/// counting it.
///
/// This is fs2's `parEvalMapUnordered(n)(read) >>> flatMap(Stream.emits) >>>
/// compile.drain` with the failure branch kept: one parent becomes many records,
/// each read may fail, and a failure — of the read or of one record — is named
/// on standard error while the stream carries on. Returns the records lost.
pub(crate) async fn expand<I, T, F>(
    items: impl Stream<Item = Result<I, CliError>>,
    fan_out: usize,
    emit: &mut crate::emit::Emitter,
    read: F,
) -> Result<usize, CliError>
where
    T: serde::Serialize,
    F: std::ops::AsyncFn(I) -> Result<Vec<Result<T, CliError>>, CliError> + Copy,
{
    let reads = items
        .map(|item| async move { read(item?).await })
        .buffer_unordered(fan_out.max(1));
    futures_util::pin_mut!(reads);
    let mut dropped = 0;
    while let Some(read) = reads.next().await {
        let rows = match read {
            Ok(rows) => rows,
            Err(error) => {
                crate::error::report(&error);
                dropped += 1;
                continue;
            }
        };
        for row in rows {
            match row {
                Ok(row) => emit.write(&row).await?,
                Err(error) => {
                    crate::error::report(&error);
                    dropped += 1;
                }
            }
        }
    }
    Ok(dropped)
}

/// [`expand`] for the levels whose parent yields records that cannot fail one
/// by one: one read, many rows, all of them kept.
pub(crate) async fn stage<I, T, F>(
    items: impl Stream<Item = Result<I, CliError>>,
    fan_out: usize,
    emit: &mut crate::emit::Emitter,
    read: F,
) -> Result<usize, CliError>
where
    T: serde::Serialize,
    F: std::ops::AsyncFn(I) -> Result<Vec<T>, CliError> + Copy,
{
    expand(items, fan_out, emit, |item| async move {
        read(item)
            .await
            .map(|rows| rows.into_iter().map(Ok::<_, CliError>).collect())
    })
    .await
}

/// A command's records: each record, or the failure that replaced it.
pub(crate) type Records<'a, B> = LocalBoxStream<'a, Result<B, CliError>>;

/// What a failure is reported under: the parent record's resource name.
pub(crate) trait Reference {
    fn reference(&self) -> &str;
}

/// `map` with `fan_out` reads in flight: one record per parent.
pub(crate) fn map<'a, A, B, E, F, Fut>(
    parents: impl Stream<Item = Result<A, CliError>> + 'a,
    fan_out: usize,
    read: F,
) -> Records<'a, B>
where
    A: Reference + 'a,
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
                let reference = parent.reference().to_owned();
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
    A: Reference + 'a,
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
                let reference = parent.reference().to_owned();
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
pub(crate) fn flat_map<'a, A, B, E, S, F>(
    parents: impl Stream<Item = Result<A, CliError>> + 'a,
    fan_out: usize,
    read: F,
) -> Records<'a, B>
where
    A: Reference + 'a,
    B: 'a,
    E: Into<CliError> + 'a,
    S: Stream<Item = Result<B, E>> + 'a,
    F: Fn(A) -> S + Clone + 'a,
{
    parents
        .map(move |parent| match parent {
            Ok(parent) => {
                let reference = parent.reference().to_owned();
                read(parent)
                    .map_err(move |cause| crate::error::context(&reference, cause))
                    .boxed_local()
            }
            Err(error) => stream::iter([Err(error)]).boxed_local(),
        })
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
