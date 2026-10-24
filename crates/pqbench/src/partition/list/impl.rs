//! The `partition::list` internals: stream a table's commits as commit-time
//! windows.

use std::collections::BTreeMap;

use futures_util::{Stream, StreamExt, TryStreamExt};
use serde_json::Value;

use crate::table::{self, LoadRequest};
use crate::v1;
use crate::Lake;

use super::api::{Error, Partition, PartitionStream};

pub(crate) async fn list(
    lake: &Lake,
    table: &str,
    uri: &str,
    env: &BTreeMap<String, String>,
    window_millis: i64,
    last_millis: Option<i64>,
) -> Result<PartitionStream<'static>, Error> {
    if window_millis <= 0 {
        return Err(Error::Invalid(
            "partition window must be positive".to_string(),
        ));
    }
    if last_millis.is_some_and(|last| last <= 0) {
        return Err(Error::Invalid(
            "partition read window must be positive".to_string(),
        ));
    }
    let request = LoadRequest::new(uri.to_string(), None, env.clone()).without_files();
    let commits = table::stream_commits(lake, &request)
        .await
        .map_err(Error::Table)?;
    Ok(windows(
        commits,
        window_millis,
        last_millis,
        table.to_string(),
        uri.to_string(),
    )
    .boxed())
}

/// What a windowed read holds while it runs: the commits still to read, and the
/// window being filled — each commit with the `commitInfo` the log gave it. A
/// window is handed over as soon as a commit falls outside it, so nothing
/// accumulates but that one window.
struct Windows {
    commit_stream: table::CommitStream<'static>,
    window_millis: i64,
    /// The read window, if any: stop reading once a commit is older than this
    /// reaches from the newest dated commit.
    last_millis: Option<i64>,
    /// The newest dated commit's time, once seen; the read window is measured
    /// back from it.
    head_time: Option<i64>,
    /// The first window edge the read window covers whole; a window below it is
    /// dropped, never truncated, so it never under-counts.
    horizon_first_time: Option<i64>,
    table: String,
    uri: String,
    open: Option<Window>,
}

/// A window being filled: its key, and the commits that fall in it, each with
/// the `commitInfo` the log gave it.
struct Window {
    first_time: i64,
    commits: Vec<(u64, Option<Value>)>,
}

/// Stream the commits as partitions, one per window, each yielded as it closes.
fn windows(
    commit_stream: table::CommitStream<'static>,
    window_millis: i64,
    last_millis: Option<i64>,
    table: String,
    uri: String,
) -> impl Stream<Item = Result<Partition, Error>> {
    let state = Windows {
        commit_stream,
        window_millis,
        last_millis,
        head_time: None,
        horizon_first_time: None,
        table,
        uri,
        open: None,
    };
    futures_util::stream::try_unfold(state, Windows::next)
}

impl Windows {
    /// The next partition: the open window once a commit falls outside it, or the
    /// last window when the commits end or the read window is reached.
    async fn next(mut self) -> Result<Option<(Partition, Self)>, Error> {
        while let Some(commit) = self.commit_stream.try_next().await.map_err(Error::Table)? {
            let Some(commit_time) = commit.commit_time else {
                continue;
            };
            self.learn_horizon(commit_time);
            if self.horizon_crossed(commit_time) {
                break;
            }
            let first_time = commit_time.div_euclid(self.window_millis) * self.window_millis;
            let entry = (commit.version, commit.commit_info);
            match self.open.take() {
                Some(mut window) if first_time >= window.first_time => {
                    window.commits.push(entry);
                    self.open = Some(window);
                }
                Some(window) => {
                    self.open = Some(Window {
                        first_time,
                        commits: vec![entry],
                    });
                    return self.close(window);
                }
                None => {
                    self.open = Some(Window {
                        first_time,
                        commits: vec![entry],
                    })
                }
            }
        }
        match self.open.take() {
            Some(window) => self.close(window),
            None => Ok(None),
        }
    }

    /// Note the newest dated commit and, from it, the read window: the oldest
    /// time it reaches, and the first window edge that time covers whole.
    fn learn_horizon(&mut self, commit_time: i64) {
        if self.head_time.is_some() {
            return;
        }
        self.head_time = Some(commit_time);
        self.horizon_first_time = self
            .last_millis
            .map(|last| window_edge(commit_time - last, self.window_millis));
    }

    /// Whether `commit_time` is older than the read window reaches. The walk is
    /// newest first, so this is the point to stop reading.
    fn horizon_crossed(&self, commit_time: i64) -> bool {
        let (Some(last), Some(head)) = (self.last_millis, self.head_time) else {
            return false;
        };
        commit_time < head - last
    }

    /// Hand over a closed window, or end the stream when the read window does
    /// not cover it whole.
    fn close(self, window: Window) -> Result<Option<(Partition, Self)>, Error> {
        if self
            .horizon_first_time
            .is_some_and(|first| window.first_time < first)
        {
            return Ok(None);
        }
        Ok(Some((self.partition(window), self)))
    }

    fn partition(&self, window: Window) -> Partition {
        // The walk reads newest first, so a window's commits arrive in reverse.
        let mut commits = window.commits;
        commits.sort_unstable_by_key(|(version, _)| *version);
        let (commit_versions, infos): (Vec<u64>, Vec<Option<Value>>) = commits.into_iter().unzip();
        // Index-aligned with the versions, with a null where a format gave none.
        let commit_info = if infos.iter().any(Option::is_some) {
            infos
                .into_iter()
                .map(|info| info.unwrap_or(Value::Null))
                .collect()
        } else {
            Vec::new()
        };
        Partition {
            name: v1::name::partition_name(&self.table, &window.first_time.to_string()),
            start_time: u64::try_from(window.first_time).ok(),
            end_time: u64::try_from(window.first_time + self.window_millis).ok(),
            commit_versions,
            commit_info,
            uri: (!self.uri.is_empty()).then(|| self.uri.clone()),
            credentials: None,
        }
    }
}

/// Round `horizon` up to the next window edge.
fn window_edge(horizon: i64, window_millis: i64) -> i64 {
    let remainder = horizon.rem_euclid(window_millis);
    if remainder == 0 {
        horizon
    } else {
        horizon + (window_millis - remainder)
    }
}
