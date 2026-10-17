//! The `partition::ls` internals: fold the commit stream into commit-time
//! windows, emitting each one as the stream crosses its boundary.

use std::collections::BTreeMap;

use futures_util::TryStreamExt;

use crate::table::{self, LoadRequest};
use crate::v1;
use crate::Lake;

use super::api::{Error, Partition};

pub(crate) async fn list(
    lake: &Lake,
    table: &str,
    uri: &str,
    env: &BTreeMap<String, String>,
    window_millis: i64,
    last_millis: Option<i64>,
) -> Result<Vec<Partition>, Error> {
    if window_millis <= 0 {
        return Err(Error::from("partition window must be positive".to_string()));
    }
    if last_millis.is_some_and(|last| last <= 0) {
        return Err(Error::from(
            "partition read window must be positive".to_string(),
        ));
    }
    let mut request = LoadRequest::new(uri.to_string(), None, env.clone()).without_files();
    if let Some(last) = last_millis {
        request = request.with_commit_window_millis(last);
    }
    let credentials = v1::Credentials::from_env(env);
    let mut windows = Windows::new(
        window_millis,
        last_millis,
        table,
        uri,
        (!credentials.is_empty()).then_some(credentials),
    );
    let mut commits = table::stream_commits(lake, &request)
        .await
        .map_err(|error| Error::from(error.to_string()))?;
    while let Some(commit) = commits
        .try_next()
        .await
        .map_err(|error| Error::from(error.to_string()))?
    {
        windows.push(commit);
    }
    Ok(windows.finish())
}

/// Folds commits into epoch-aligned half-open windows as they stream in.
///
/// Commits arrive newest first, so a window closes as soon as a commit crosses
/// into an older one: its [`Partition`] is pushed and its commits dropped. A
/// window is dropped instead when the optional horizon does not cover it whole,
/// so a window never under-counts.
struct Windows<'a> {
    window_millis: i64,
    horizon_millis: Option<i64>,
    /// The aligned horizon, known once the newest dated commit is seen.
    min_first_time: Option<i64>,
    head_time: Option<i64>,
    table: &'a str,
    uri: &'a str,
    credentials: Option<v1::Credentials>,
    current: Option<(i64, Vec<u64>)>,
    done: Vec<Partition>,
}

impl<'a> Windows<'a> {
    fn new(
        window_millis: i64,
        horizon_millis: Option<i64>,
        table: &'a str,
        uri: &'a str,
        credentials: Option<v1::Credentials>,
    ) -> Self {
        Self {
            window_millis,
            horizon_millis,
            min_first_time: None,
            head_time: None,
            table,
            uri,
            credentials,
            current: None,
            done: Vec::new(),
        }
    }

    fn push(&mut self, commit: table::LogCommit) {
        let Some(time) = commit.commit_time else {
            return;
        };
        if self.head_time.is_none() {
            self.head_time = Some(time);
            self.min_first_time = self.horizon_millis.map(|last| self.align_up(time - last));
        }
        let first_time = time.div_euclid(self.window_millis) * self.window_millis;
        match self.current.take() {
            Some((open, mut versions)) if first_time >= open => {
                versions.push(commit.version);
                self.current = Some((open, versions));
            }
            Some((open, versions)) => {
                self.emit(open, versions);
                self.current = Some((first_time, vec![commit.version]));
            }
            None => self.current = Some((first_time, vec![commit.version])),
        }
    }

    fn emit(&mut self, first_time: i64, commit_versions: Vec<u64>) {
        if self.min_first_time.is_some_and(|min| first_time < min) {
            return;
        }
        self.done.push(Partition {
            name: v1::name::partition_name(self.table, &first_time.to_string()),
            start_time: u64::try_from(first_time).ok(),
            end_time: u64::try_from(first_time + self.window_millis).ok(),
            commit_versions,
            uri: (!self.uri.is_empty()).then(|| self.uri.to_string()),
            credentials: self.credentials.clone(),
        });
    }

    /// The horizon rounded up to a window edge.
    fn align_up(&self, horizon: i64) -> i64 {
        let remainder = horizon.rem_euclid(self.window_millis);
        if remainder == 0 {
            horizon
        } else {
            horizon + (self.window_millis - remainder)
        }
    }

    fn finish(mut self) -> Vec<Partition> {
        if let Some((open, versions)) = self.current.take() {
            self.emit(open, versions);
        }
        // Windows closed newest first; the walk reads oldest first.
        self.done.reverse();
        self.done
    }
}
