//! Delta snapshot resolution: the transaction log and the active files.
//!
//! [`load`] is the only entry point. It does not read Parquet footers; pipe the
//! document to `bytemass` to measure. delta-rs stays in this module.
//!
//! The `delta` feature compiles the loader. Without it [`load`] fails and names
//! the feature. There are no feature flags outside this file.

use super::{LoadRequest, TableInfo};

/// Load the transaction log and the active files of a Delta table.
///
/// # Errors
/// Fails when the `delta` feature is off, the log cannot be read, the snapshot
/// is invalid, or a data path leaves the table root.
pub async fn load(request: &LoadRequest) -> Result<TableInfo, super::Error> {
    #[cfg(feature = "delta")]
    {
        resolve::load(request)
            .await
            .map_err(|error| super::Error(error.to_string()))
    }
    #[cfg(not(feature = "delta"))]
    {
        let _ = request;
        Err(super::Error(
            "delta tables require the `delta` feature (`delta-s3` for S3)".into(),
        ))
    }
}

#[cfg(feature = "delta")]
mod resolve {
    use std::collections::BTreeMap;
    use std::path::{Component, Path, PathBuf};

    use deltalake::logstore::LogStore;
    use deltalake::{DeltaTable, DeltaTableBuilder};
    use futures::TryStreamExt;
    use url::Url;

    use super::super::{LoadRequest, LogAction, LogCommit, TableFile, TableFormat, TableInfo};

    #[derive(Debug)]
    pub(super) struct Error(String);

    impl std::fmt::Display for Error {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "delta: {}", self.0)
        }
    }

    impl std::error::Error for Error {}

    pub(super) async fn load(request: &LoadRequest) -> Result<TableInfo, Error> {
        if request.version.is_some() && request.selection.snapshot_time() {
            return Err(Error(
                "use --version or --exclude-snapshot-before/--exclude-snapshot-after, not both"
                    .into(),
            ));
        }
        let table = open(&request.uri, request.version, &request.env).await?;
        let snapshot = snapshot_info(&table)?;
        let version = match request.version {
            Some(version) => version,
            None if request.selection.snapshot_time() => {
                snapshot_version_for_time(&table, snapshot.version, &request.selection)?
            }
            None => snapshot.version,
        };
        let table = if version == snapshot.version {
            table
        } else {
            open(&request.uri, Some(version), &request.env).await?
        };
        let snapshot = snapshot_info(&table)?;
        let mut files = active_files(&table).await?;
        let log = read_log(&table, snapshot.version).await?;
        annotate_add_versions(&mut files, &log);
        let snapshot_time = table
            .snapshot()
            .ok()
            .and_then(|snap| snap.version_timestamp(snapshot.version))
            .and_then(crate::object_store::unix_millis_rfc3339);
        Ok(TableInfo {
            kind: "pqbench.table".into(),
            version: 1,
            format: TableFormat::DELTA,
            uri: request.uri.clone(),
            snapshot_version: snapshot.version,
            snapshot_time,
            selection: request.selection.clone(),
            partition_columns: snapshot.partition_columns,
            log,
            files,
            env: request.env.clone(),
        })
    }

    fn snapshot_version_for_time(
        table: &DeltaTable,
        latest: u64,
        selection: &crate::pattern::Selection,
    ) -> Result<u64, Error> {
        let snapshot = table.snapshot().map_err(delta_error)?;
        let mut chosen = None;
        for version in (0..=latest).rev() {
            let Some(millis) = snapshot.version_timestamp(version) else {
                continue;
            };
            if super::super::keep_snapshot_time(millis, selection)
                .map_err(|e| Error(e.to_string()))?
            {
                chosen = Some(version);
                break;
            }
        }
        chosen.ok_or_else(|| Error("no snapshot remained after exclude".into()))
    }

    fn annotate_add_versions(files: &mut [TableFile], log: &[LogCommit]) {
        let mut versions = BTreeMap::new();
        for commit in log {
            for action in &commit.actions {
                let Some(path) = &action.path else {
                    continue;
                };
                match action.kind.as_str() {
                    "add" => {
                        versions.insert(path.clone(), commit.version);
                    }
                    "remove" => {
                        versions.remove(path);
                    }
                    _ => {}
                }
            }
        }
        for file in files {
            file.snapshot_version = versions.get(&file.path).copied();
        }
    }

    async fn open(
        uri: &str,
        version: Option<u64>,
        env: &BTreeMap<String, String>,
    ) -> Result<DeltaTable, Error> {
        if uri.contains("://") {
            let url = Url::parse(uri).map_err(|e| Error(format!("invalid table URI: {e}")))?;
            if url.scheme() == "file" {
                let path = url
                    .to_file_path()
                    .map_err(|()| Error("invalid local table URI".into()))?;
                return load_local_table(&local_root(&path)?, version, env).await;
            }
            return load_table(url, version, env).await;
        }
        load_local_table(&local_root(Path::new(uri))?, version, env).await
    }

    async fn read_log(table: &DeltaTable, last_version: u64) -> Result<Vec<LogCommit>, Error> {
        let store = table.log_store();
        let mut commits = Vec::new();
        for version in 0..=last_version {
            let Some(bytes) = store
                .read_commit_entry(version)
                .await
                .map_err(delta_error)?
            else {
                continue;
            };
            commits.push(LogCommit {
                version,
                actions: parse_commit(&bytes, version)?,
            });
        }
        Ok(commits)
    }

    fn parse_commit(bytes: &[u8], version: u64) -> Result<Vec<LogAction>, Error> {
        let text = std::str::from_utf8(bytes)
            .map_err(|e| Error(format!("commit {version} is not UTF-8: {e}")))?;
        text.lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| parse_action(line, version))
            .collect()
    }

    fn parse_action(line: &str, version: u64) -> Result<LogAction, Error> {
        let value: serde_json::Value = serde_json::from_str(line)
            .map_err(|e| Error(format!("cannot parse commit {version}: {e}")))?;
        let object = value
            .as_object()
            .ok_or_else(|| Error(format!("commit {version} action is not an object")))?;
        let (kind, body) = object
            .iter()
            .next()
            .ok_or_else(|| Error(format!("commit {version} action is empty")))?;
        Ok(LogAction {
            kind: kind.clone(),
            path: body
                .get("path")
                .and_then(|path| path.as_str())
                .map(str::to_owned),
        })
    }

    struct SnapshotInfo {
        version: u64,
        partition_columns: Vec<String>,
    }

    fn local_root(path: &Path) -> Result<PathBuf, Error> {
        let root = path
            .canonicalize()
            .map_err(|e| Error(format!("cannot open table {}: {e}", path.display())))?;
        if !root.join("_delta_log").is_dir() {
            return Err(Error(format!("missing _delta_log in {}", root.display())));
        }
        Ok(root)
    }

    async fn load_local_table(
        root: &Path,
        version: Option<u64>,
        env: &BTreeMap<String, String>,
    ) -> Result<DeltaTable, Error> {
        let url = Url::from_directory_path(root)
            .map_err(|()| Error("cannot convert table path to a local file URL".into()))?;
        load_table(url, version, env).await
    }

    async fn load_table(
        url: Url,
        version: Option<u64>,
        env: &BTreeMap<String, String>,
    ) -> Result<DeltaTable, Error> {
        let mut builder = DeltaTableBuilder::from_url(url).map_err(delta_error)?;
        if !env.is_empty() {
            builder = builder.with_storage_options(env.clone().into_iter().collect());
        }
        if let Some(version) = version {
            builder = builder.with_version(version);
        }
        builder.load().await.map_err(delta_error)
    }

    fn snapshot_info(table: &DeltaTable) -> Result<SnapshotInfo, Error> {
        let snapshot = table.snapshot().map_err(delta_error)?;
        Ok(SnapshotInfo {
            version: snapshot.version(),
            partition_columns: snapshot.metadata().partition_columns().to_vec(),
        })
    }

    /// Resolve every active data file to a path `bytemass` can read.
    async fn active_files(table: &DeltaTable) -> Result<Vec<TableFile>, Error> {
        let root = if table.table_url().scheme() == "file" {
            Some(
                table
                    .table_url()
                    .to_file_path()
                    .map_err(|()| Error("invalid local table URI".into()))?,
            )
        } else {
            None
        };
        let mut files = table.get_active_add_actions_by_partitions(&[]);
        let mut active = vec![];
        while let Some(file) = files.try_next().await.map_err(delta_error)? {
            let relative = file.path().to_string();
            let size = u64::try_from(file.size())
                .map_err(|_| Error(format!("invalid file size in log: {relative}")))?;
            let uri = match &root {
                Some(root) => local_uri(root, &relative)?,
                None => object_uri(table.table_url(), &relative)?,
            };
            active.push(TableFile {
                path: relative,
                uri,
                size,
                last_modified_time: crate::object_store::unix_millis_rfc3339(
                    file.modification_time(),
                ),
                snapshot_version: None,
            });
        }
        Ok(active)
    }

    /// A data path that stays inside the table: empty, absolute, or URI paths
    /// are rejected here, once, for both local and object adapters.
    fn relative_data_path(relative: &str) -> Result<&Path, Error> {
        let path = Path::new(relative);
        if relative.is_empty()
            || relative.contains("://")
            || !path.components().all(|c| matches!(c, Component::Normal(_)))
        {
            return Err(Error(format!(
                "only relative data paths inside the table are supported: {relative}"
            )));
        }
        Ok(path)
    }

    fn object_uri(base: &Url, relative: &str) -> Result<String, Error> {
        relative_data_path(relative)?;
        let mut directory = base.clone();
        if !directory.path().ends_with('/') {
            directory.set_path(&format!("{}/", directory.path()));
        }
        Ok(directory
            .join(relative)
            .map_err(|e| Error(format!("invalid active file path {relative}: {e}")))?
            .into())
    }

    /// Join a local active file without opening it. Size is the log's claim;
    /// `bytemass` compares the measured size later.
    fn local_uri(root: &Path, relative: &str) -> Result<String, Error> {
        let path = relative_data_path(relative)?;
        Ok(root.join(path).to_string_lossy().into_owned())
    }

    fn delta_error(error: deltalake::DeltaTableError) -> Error {
        Error(format!("cannot resolve snapshot: {error}"))
    }
}
