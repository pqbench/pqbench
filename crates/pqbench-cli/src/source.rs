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
//! A one-record level reads a batch of `fan_out` parents to completion before
//! it writes the batch's first record. A streaming level multiplexes up to
//! `fan_out` core streams in this task — no task per stream, no channel between
//! them — and each core stream reads a batch, writes it, and only then reads
//! again. Either way the sink is the only poller: a consumer blocked on a write
//! stops every read, which is what bounds the run's memory; the trade is one
//! straggler per batch, and a read's timeout running while the sink stalls.

use std::future::Future;

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
        let record = next_record(command, &mut lines).await?;
        Ok(record.map(|record| (record, lines)))
    });
    Box::pin(first.chain(rest))
}

/// Standard input's next record: the next line that is not blank, or `None`
/// at the end of input.
async fn next_record(
    command: &'static str,
    lines: &mut Lines<BufReader<tokio::io::Stdin>>,
) -> Result<Option<Value>, CliError> {
    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        let record: Value = serde_json::from_str(&line)
            .map_err(|error| format!("{command} reads NDJSON records: {error}"))?;
        return Ok(Some(record));
    }
    Ok(None)
}

/// Read stdin's first record (a parent ref, kept for the command's stream),
/// without resolving config.
pub(crate) async fn read_input(command: &'static str) -> Result<Input, CliError> {
    let piped = is_stream();
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let first = if piped {
        next_record(command, &mut lines).await?
    } else {
        None
    };
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
    parents
        .chunks(fan_out.max(1))
        .then(move |batch| {
            let read = read.clone();
            async move { read_batch(batch, read).await }
        })
        .flat_map(stream::iter)
        .filter_map(|row| async move {
            match row {
                Ok(Some(row)) => Some(Ok(row)),
                Ok(None) => None,
                Err(error) => Some(Err(error)),
            }
        })
        .boxed_local()
}

/// Read one batch of parents to completion: each parent's read is one future,
/// and a parent that failed to parse is its own failed item. The batch's rows
/// are joined in input order.
async fn read_batch<A, B, E, F, Fut>(
    batch: Vec<Result<A, CliError>>,
    read: F,
) -> Vec<Result<Option<B>, CliError>>
where
    A: Resource,
    E: Into<CliError>,
    F: Fn(A) -> Fut + Clone,
    Fut: Future<Output = Result<Option<B>, E>>,
{
    let reads = batch.into_iter().map(|record| {
        let read = read.clone();
        async move {
            let parent = record?;
            let reference = parent.name().to_owned();
            read(parent)
                .await
                .map_err(|cause| crate::error::context(&reference, cause))
        }
    });
    join_all(reads).await
}

/// `flatMap` with `fan_out` streams in flight: a parent becomes its stream,
/// and `flatten_unordered` polls those streams concurrently in this task.
///
/// A parent's `read` is awaited inside its own one-item stream, so pulling the
/// next parent never waits on the last one's request: `flatten_unordered` polls
/// every pending read on each pass. It takes the next parent once the number of
/// streams in flight drops below `fan_out`.
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
            stream::once(async move {
                let parent = parent?;
                let reference = parent.name().to_owned();
                let rows = read(parent).await?;
                let rows: LocalBoxStream<'a, Result<B, CliError>> = rows
                    .map_err(move |error| crate::error::context(&reference, error))
                    .boxed_local();
                Ok::<_, CliError>(rows)
            })
            .try_flatten()
            .boxed_local()
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
            Ok(row) => {
                if emit.write(&row).await?.is_break() {
                    break;
                }
            }
            Err(error) => {
                crate::error::report(&error);
                dropped += 1;
            }
        }
    }
    emit.finish().await?;
    crate::error::fail_when_dropped(dropped)
}
