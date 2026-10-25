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
//! A level reads a batch of `fan_out` parents to completion before it writes
//! the batch's first record. A slow consumer then backpressures the pipe with
//! no read in flight, so a request timeout cannot fire on a read the consumer
//! stalled; the trade is one straggler per batch.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;

use futures_util::future::join_all;
use futures_util::stream::{self, LocalBoxStream, Stream, StreamExt};
use futures_util::TryStreamExt;
use serde_json::Value;
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

/// `map` over batches of parents: one record per parent.
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
    filter_map(parents, fan_out, move |parent| {
        let read = read.clone();
        async move { read(parent).await.map(Some) }
    })
}

/// A parent keeps one record or none; a batch is read whole before it is
/// written, so a stalled consumer leaves no read in flight.
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
    let parents: Pin<Box<dyn Stream<Item = Result<A, CliError>> + 'a>> = Box::pin(parents);
    stream::unfold(
        (parents, VecDeque::new(), false),
        move |(mut parents, mut rows, mut done)| {
            let read = read.clone();
            async move {
                loop {
                    if let Some(row) = rows.pop_front() {
                        return Some((row, (parents, rows, done)));
                    }
                    if done {
                        return None;
                    }
                    let mut batch = Vec::with_capacity(fan_out.max(1));
                    while batch.len() < fan_out.max(1) {
                        match parents.as_mut().next().await {
                            Some(record) => batch.push(record),
                            None => {
                                done = true;
                                break;
                            }
                        }
                    }
                    let mut reads = Vec::with_capacity(batch.len());
                    for record in batch {
                        match record {
                            Ok(parent) => {
                                let reference = parent.name().to_owned();
                                let read = read.clone();
                                reads.push(async move {
                                    read(parent).await.map_err(move |cause| {
                                        crate::error::context(&reference, cause)
                                    })
                                });
                            }
                            Err(error) => rows.push_back(Err(error)),
                        }
                    }
                    rows.extend(
                        join_all(reads)
                            .await
                            .into_iter()
                            .filter_map(|row| match row {
                                Ok(Some(row)) => Some(Ok(row)),
                                Ok(None) => None,
                                Err(error) => Some(Err(error)),
                            }),
                    );
                }
            }
        },
    )
    .boxed_local()
}

/// `flatMap` over batches of parents: a parent becomes its stream, and every
/// parent read in a batch completes before its first row is written.
///
/// `read` may fail before it yields anything (the table it loads, a window it
/// rejects); that failure is reported under the parent, like a stream item.
/// A returned stream must not leave a read pending across items: the sink may
/// block on a slow consumer between them.
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
    let parents: Pin<Box<dyn Stream<Item = Result<A, CliError>> + 'a>> = Box::pin(parents);
    let flat: Option<LocalBoxStream<'a, Result<B, CliError>>> = None;
    stream::unfold(
        (parents, flat, false),
        move |(mut parents, mut flat, mut done)| {
            let read = read.clone();
            async move {
                loop {
                    if let Some(rows) = flat.as_mut() {
                        if let Some(row) = rows.next().await {
                            return Some((row, (parents, flat, done)));
                        }
                    }
                    if done {
                        return None;
                    }
                    let mut batch = Vec::with_capacity(fan_out.max(1));
                    while batch.len() < fan_out.max(1) {
                        match parents.as_mut().next().await {
                            Some(record) => batch.push(record),
                            None => {
                                done = true;
                                break;
                            }
                        }
                    }
                    if batch.is_empty() {
                        return None;
                    }
                    let mut reads = Vec::with_capacity(batch.len());
                    let mut errors = VecDeque::new();
                    for record in batch {
                        match record {
                            Ok(parent) => {
                                let reference = parent.name().to_owned();
                                let read = read.clone();
                                reads.push(async move {
                                    read(parent).await.map_err(move |cause| {
                                        crate::error::context(&reference, cause)
                                    })
                                });
                            }
                            Err(error) => errors.push_back(Err(error)),
                        }
                    }
                    let mut streams = Vec::new();
                    for result in join_all(reads).await {
                        match result {
                            Ok(stream) => {
                                let stream: LocalBoxStream<'a, Result<B, CliError>> =
                                    stream.map_err(Into::into).boxed_local();
                                streams.push(stream);
                            }
                            Err(error) => errors.push_back(Err(error)),
                        }
                    }
                    let streamed = stream::iter(streams).flatten_unordered(Some(fan_out.max(1)));
                    flat = Some(stream::iter(errors).chain(streamed).boxed_local());
                }
            }
        },
    )
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
