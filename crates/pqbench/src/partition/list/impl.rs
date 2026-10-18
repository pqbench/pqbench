//! The `partition::list` internals: stream a table's commits as commit-time
//! windows.

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
    credentials: Option<&v1::Credentials>,
    window_millis: i64,
) -> Result<PartitionStream<'static>, Error> {
    if window_millis <= 0 {
        return Err(Error::from("partition window must be positive".to_string()));
    }
    let env = credentials.map(v1::Credentials::to_env).unwrap_or_default();
    let request = LoadRequest::new(uri.to_string(), None, env).without_files();
    let credentials = credentials.filter(|lease| !lease.is_empty()).cloned();
    let commits = table::stream_commits(lake, &request)
        .await
        .map_err(|error| Error::from(error.to_string()))?;
    Ok(windows(
        commits,
        window_millis,
        table.to_string(),
        uri.to_string(),
        credentials,
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
    table: String,
    uri: String,
    credentials: Option<v1::Credentials>,
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
    table: String,
    uri: String,
    credentials: Option<v1::Credentials>,
) -> impl Stream<Item = Result<Partition, Error>> {
    let state = Windows {
        commit_stream,
        window_millis,
        table,
        uri,
        credentials,
        open: None,
    };
    futures_util::stream::try_unfold(state, Windows::next)
}

impl Windows {
    /// The next partition: the open window once a commit falls outside it, or the
    /// last window when the commits end.
    async fn next(mut self) -> Result<Option<(Partition, Self)>, Error> {
        while let Some(commit) = self
            .commit_stream
            .try_next()
            .await
            .map_err(|error| Error::from(error.to_string()))?
        {
            let Some(commit_time) = commit.commit_time else {
                continue;
            };
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
                    return Ok(Some((self.partition(window), self)));
                }
                None => {
                    self.open = Some(Window {
                        first_time,
                        commits: vec![entry],
                    })
                }
            }
        }
        Ok(self
            .open
            .take()
            .map(|window| (self.partition(window), self)))
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
            credentials: self.credentials.clone(),
        }
    }
}
