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
//! A one-record level keeps a sliding window of `fan_out` reads in flight: a
//! parent's slot is freed the moment its read completes, not when the batch it
//! belongs to does, so one slow parent no longer idles the other slots. The
//! records are still written in input order, and at most `fan_out` finished
//! records wait ahead of the head, so a stalled consumer leaves no read running
//! for long. A streaming level multiplexes up to `fan_out` core streams in this
//! task — no task per stream, no channel between them. Either way the sink is
//! the only poller: a consumer blocked on a write stops every read, which is
//! what bounds the run's memory.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;

use futures_util::stream::{self, FuturesUnordered, LocalBoxStream, Stream, StreamExt};
use futures_util::TryStreamExt;
use pqbench::third_party::json;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, BufReader};

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
    pub lines: Lines,
}

/// Standard input's lines, read as bytes.
///
/// A record is parsed where it lies, in a buffer that is reused, so reading
/// one costs no string of its own.
pub(crate) struct Lines {
    input: BufReader<tokio::io::Stdin>,
    /// The bytes of the line being parsed.
    bytes: Vec<u8>,
}

impl Lines {
    fn new() -> Self {
        Self {
            input: BufReader::new(tokio::io::stdin()),
            bytes: Vec::new(),
        }
    }

    /// The next line, without its newline: `None` at the end of input.
    async fn next(&mut self) -> Result<Option<&mut [u8]>, CliError> {
        self.bytes.clear();
        if self.input.read_until(b'\n', &mut self.bytes).await? == 0 {
            return Ok(None);
        }
        if self.bytes.last() == Some(&b'\n') {
            self.bytes.pop();
        }
        Ok(Some(self.bytes.as_mut_slice()))
    }
}

/// The parent records: the peeked first record, then the rest of stdin.
pub(crate) fn records(
    command: &'static str,
    first: Option<Value>,
    lines: Lines,
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
async fn next_record(command: &'static str, lines: &mut Lines) -> Result<Option<Value>, CliError> {
    while let Some(line) = lines.next().await? {
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let record = json::from_slice_mut(line)
            .map_err(|error| format!("{command} reads NDJSON records: {error}"))?;
        return Ok(Some(record));
    }
    Ok(None)
}

/// Read stdin's first record (a parent ref, kept for the command's stream),
/// without resolving config.
pub(crate) async fn read_input(command: &'static str) -> Result<Input, CliError> {
    let piped = is_stream();
    let mut lines = Lines::new();
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

/// `map` over parents: one record per parent, in input order.
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

/// A parent keeps one record or none. At most `fan_out` reads run at once, and
/// a finished read frees its slot immediately: pulling the next parent never
/// waits for the head's read, only for the head's record to be written.
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
    let window = Window {
        upstream: parents.boxed_local(),
        read,
        fan_out: fan_out.max(1),
        pending: FuturesUnordered::new(),
        ready: BTreeMap::new(),
        next_parent: 0,
        next_row: 0,
        exhausted: false,
    };
    stream::unfold(window, |mut window| async move {
        loop {
            if let Some(row) = window.ready.remove(&window.next_row) {
                window.next_row += 1;
                return Some((row, window));
            }
            window.fill().await;
            if window.pending.is_empty() && window.exhausted {
                return None;
            }
            if let Some((index, row)) = window.pending.next().await {
                window.ready.insert(index, row);
            }
        }
    })
    .filter_map(|row| async move {
        match row {
            Ok(Some(row)) => Some(Ok(row)),
            Ok(None) => None,
            Err(error) => Some(Err(error)),
        }
    })
    .boxed_local()
}

/// One read's result, tagged with its parent's index so the stream can yield
/// its records in input order.
type ReadFuture<'a, B> = Pin<Box<dyn Future<Output = (usize, Result<Option<B>, CliError>)> + 'a>>;

/// The sliding window behind [`filter_map`]: the parents still to read, the
/// reads in flight, and the finished records waiting for their turn.
///
/// At most `fan_out` reads are in flight, so a completed read frees its slot at
/// once. At most `fan_out` finished records wait, so a head that never finishes
/// cannot buffer the whole parent stream: the window stops pulling once its
/// finished side is full.
struct Window<'a, A, B, F> {
    upstream: LocalBoxStream<'a, Result<A, CliError>>,
    read: F,
    fan_out: usize,
    pending: FuturesUnordered<ReadFuture<'a, B>>,
    ready: BTreeMap<usize, Result<Option<B>, CliError>>,
    next_parent: usize,
    next_row: usize,
    exhausted: bool,
}

impl<'a, A, B, E, F, Fut> Window<'a, A, B, F>
where
    A: Resource + 'a,
    B: 'a,
    E: Into<CliError> + 'a,
    F: Fn(A) -> Fut + Clone + 'a,
    Fut: Future<Output = Result<Option<B>, E>> + 'a,
{
    /// Start the parents the window has room for.
    async fn fill(&mut self) {
        while !self.exhausted
            && self.pending.len() < self.fan_out
            && self.ready.len() < self.fan_out
        {
            let Some(parent) = self.upstream.next().await else {
                self.exhausted = true;
                break;
            };
            let index = self.next_parent;
            self.next_parent += 1;
            let read = self.read.clone();
            self.pending.push(Box::pin(
                async move { (index, read_parent(read, parent).await) },
            ));
        }
    }
}

/// Read one parent, naming a failure under the parent it came from.
async fn read_parent<A, B, E, F, Fut>(
    read: F,
    parent: Result<A, CliError>,
) -> Result<Option<B>, CliError>
where
    A: Resource,
    E: Into<CliError>,
    F: Fn(A) -> Fut + Clone,
    Fut: Future<Output = Result<Option<B>, E>>,
{
    let parent = parent?;
    let reference = parent.name().to_owned();
    read(parent)
        .await
        .map_err(|cause| crate::error::context(&reference, cause))
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

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::Poll;
    use std::time::Duration;

    use futures_util::stream::{self, Stream, StreamExt};
    use serde::{Deserialize, Serialize};
    use tokio::sync::{mpsc, Notify};

    use pqbench::v1::Resource;

    use super::filter_map;

    /// A parent that records only its name: the read's input and the row it
    /// yields, so a test can tell a fast read from a slow one.
    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct Parent {
        name: String,
    }

    impl Resource for Parent {
        const NOUN: &'static str = "parent";
        const SHAPE: &'static str = "parents/{parent}";

        fn from_name(name: &str) -> Option<Self> {
            Some(Self {
                name: name.to_string(),
            })
        }

        fn name(&self) -> &str {
            &self.name
        }

        fn name_mut(&mut self) -> &mut String {
            &mut self.name
        }
    }

    /// A read that finishes ahead of the head frees its slot at once: item 1
    /// completes while item 0 waits on a channel, and item 2 starts and
    /// completes before item 0 is released.
    #[tokio::test]
    async fn a_completed_read_starts_the_next_before_the_head() {
        let gate = Arc::new(Notify::new());
        let (finished, mut finished_rx) = mpsc::unbounded_channel::<String>();
        let read_gate = Arc::clone(&gate);
        let parents = stream::iter(["0", "1", "2"]).map(|name| {
            Ok(Parent {
                name: name.to_string(),
            })
        });
        let mut rows = Box::pin(filter_map(parents, 2, move |parent: Parent| {
            let gate = Arc::clone(&read_gate);
            let finished = finished.clone();
            async move {
                match parent.name.as_str() {
                    "0" => gate.notified().await,
                    "2" => {
                        let _ = finished.send(parent.name.clone());
                    }
                    _ => {}
                }
                Ok::<_, super::CliError>(Some(parent.name))
            }
        }));

        // Drive the window until the third parent's read completes. The head
        // is blocked, so the stream itself must stay pending: no record may be
        // written out of order.
        let head_blocked = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let completed = futures_util::future::poll_fn(|cx| {
                    assert!(
                        matches!(Pin::new(&mut rows).poll_next(cx), Poll::Pending),
                        "a blocked head must not let a later record through"
                    );
                    finished_rx.poll_recv(cx)
                })
                .await;
                if completed.is_some() {
                    break;
                }
            }
        })
        .await;
        assert!(
            head_blocked.is_ok(),
            "item 2 must start and finish while item 0 is still blocked"
        );

        gate.notify_one();
        let rest: Vec<String> = rows
            .map(|row| row.expect("every read succeeds"))
            .collect()
            .await;
        assert_eq!(rest, ["0", "1", "2"], "records keep their input order");
    }
}
