//! A lake is a list of tables. Discovery names them; it does not read logs
//! or Parquet footers. `pqbench table` loads each table, and `pqbench bytemass`
//! measures the files.

use std::collections::BTreeMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::table::{self, TableInfo};
use crate::third_party::object_store::{self, PrefixListing};

/// Errors discovering a lake.
#[derive(Debug)]
pub struct Error(String);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "lake: {}", self.0)
    }
}

impl std::error::Error for Error {}

/// One table in a lake. `info` is filled in by `pqbench table`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LakeTable {
    /// Name `pqbench table` reports this table under: a path relative to the
    /// lake root for a directory, or the Unity FQN `catalog.schema.table`.
    pub name: String,
    /// Table root `pqbench table` should load.
    pub uri: String,
    /// Storage options for this table. A pipe carries them to the next command.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
    /// Loaded table metadata. Absent until `pqbench table` runs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub info: Option<TableInfo>,
}

/// A versioned list of tables.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Lake {
    /// Document kind; always `pqbench.lake`.
    pub kind: String,
    /// Document version; currently `1`.
    pub version: u32,
    /// Lake name, usually the root directory name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Tables in name order.
    pub tables: Vec<LakeTable>,
}

/// Find every Delta or Iceberg table under `root`. A directory that contains
/// `_delta_log`, `metadata/version-hint.text`, or `metadata/*.metadata.json`
/// is a table, and its children are not searched. Delta wins when both markers
/// are present (UniForm).
///
/// # Errors
/// Fails when `root` cannot be opened or it contains no tables.
pub fn discover(root: &Path) -> Result<Lake, Error> {
    discover_bounded(root, None)
}

/// Find every Delta or Iceberg table under `root`, stopping after
/// `max_depth` path components below the root. `None` walks until a marker.
///
/// # Errors
/// Fails when `root` cannot be opened or it contains no tables.
pub fn discover_bounded(root: &Path, max_depth: Option<usize>) -> Result<Lake, Error> {
    let root = root
        .canonicalize()
        .map_err(|e| Error(format!("cannot open lake {}: {e}", root.display())))?;
    if !root.is_dir() {
        return Err(Error(format!("{} is not a directory", root.display())));
    }
    let mut tables = Vec::new();
    walk(&root, &root, 0, max_depth, &mut tables)?;
    let name = root
        .file_name()
        .map(|name| name.to_string_lossy().into_owned());
    finish(name, tables, root.display())
}

/// Find every Delta or Iceberg table under `uri`. A local path or `file://`
/// URI walks the filesystem. An `s3://` prefix lists one level at a time until
/// a table marker, and requires the `aws` feature.
///
/// # Errors
/// Fails when the location cannot be opened, listing fails, or no tables are
/// found.
pub async fn discover_uri(uri: &str, env: &BTreeMap<String, String>) -> Result<Lake, Error> {
    discover_uri_bounded(uri, env, None).await
}

/// Find every table under `uri`. `max_depth` is path components below the
/// walk root; `None` is unbounded.
///
/// # Errors
/// Fails when the location cannot be opened, listing fails, or no tables are
/// found.
pub async fn discover_uri_bounded(
    uri: &str,
    env: &BTreeMap<String, String>,
    max_depth: Option<usize>,
) -> Result<Lake, Error> {
    if is_local(uri) {
        return discover_bounded(&local_path(uri)?, max_depth);
    }
    discover_listed(uri, env, max_depth).await
}

fn walk(
    root: &Path,
    dir: &Path,
    depth: usize,
    max_depth: Option<usize>,
    tables: &mut Vec<LakeTable>,
) -> Result<(), Error> {
    if is_table(dir)? {
        tables.push(LakeTable {
            name: table_name(root, dir)?,
            uri: dir.to_string_lossy().into_owned(),
            env: BTreeMap::new(),
            info: None,
        });
        return Ok(());
    }
    if max_depth.is_some_and(|max| depth >= max) {
        return Ok(());
    }
    let entries =
        std::fs::read_dir(dir).map_err(|e| Error(format!("cannot read {}: {e}", dir.display())))?;
    for entry in entries {
        let entry = entry.map_err(|e| Error(format!("cannot read {}: {e}", dir.display())))?;
        let path = entry.path();
        if path.is_symlink() || !path.is_dir() {
            continue;
        }
        if entry.file_name().to_string_lossy().starts_with('.') {
            continue;
        }
        walk(root, &path, depth + 1, max_depth, tables)?;
    }
    Ok(())
}

fn is_table(dir: &Path) -> Result<bool, Error> {
    table::local_format(dir)
        .map(|format| format.is_some())
        .map_err(|error| Error(error.to_string()))
}

fn table_name(root: &Path, dir: &Path) -> Result<String, Error> {
    if dir == root {
        return Ok(dir
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_else(|| "table".into()));
    }
    Ok(dir
        .strip_prefix(root)
        .map_err(|e| Error(e.to_string()))?
        .to_string_lossy()
        .replace('\\', "/"))
}

async fn discover_listed(
    uri: &str,
    env: &BTreeMap<String, String>,
    max_depth: Option<usize>,
) -> Result<Lake, Error> {
    let options: Vec<(String, String)> = env
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    let root = uri.trim_end_matches('/').to_string();
    let mut tables = Vec::new();
    let mut pending = vec![(root.clone(), 0usize)];
    while let Some((dir, depth)) = pending.pop() {
        let listing = object_store::list_prefix(&dir, &options)
            .await
            .map_err(|e| Error(e.to_string()))?;
        if listing_is_delta(&listing) || listing_is_iceberg(&listing, &dir, &options).await? {
            tables.push(listed_table(&root, &dir, env));
            continue;
        }
        if max_depth.is_some_and(|max| depth >= max) {
            continue;
        }
        for prefix in listing.prefixes {
            if prefix.starts_with('.') || prefix.is_empty() {
                continue;
            }
            if prefix == "_delta_log" || prefix == "metadata" {
                continue;
            }
            pending.push((join_uri(&dir, &prefix), depth + 1));
        }
    }
    finish(uri_name(&root), tables, &root)
}

fn listed_table(root: &str, dir: &str, env: &BTreeMap<String, String>) -> LakeTable {
    LakeTable {
        name: remote_table_name(root, dir),
        uri: dir.to_string(),
        env: env.clone(),
        info: None,
    }
}

fn listing_is_delta(listing: &PrefixListing) -> bool {
    listing.prefixes.iter().any(|name| name == "_delta_log")
}

async fn listing_is_iceberg(
    listing: &PrefixListing,
    dir: &str,
    options: &[(String, String)],
) -> Result<bool, Error> {
    if !listing.prefixes.iter().any(|name| name == "metadata") {
        return Ok(false);
    }
    let metadata = object_store::list_prefix(&join_uri(dir, "metadata"), options)
        .await
        .map_err(|e| Error(e.to_string()))?;
    Ok(metadata
        .objects
        .iter()
        .any(|name| is_iceberg_metadata(name)))
}

fn is_iceberg_metadata(name: &str) -> bool {
    !name.contains('/')
        && (name == "version-hint.text"
            || (name.ends_with(".metadata.json") && name != ".metadata.json"))
}

fn remote_table_name(root: &str, dir: &str) -> String {
    let root = root.trim_end_matches('/');
    let dir = dir.trim_end_matches('/');
    if dir == root {
        return uri_name(dir).unwrap_or_else(|| "table".into());
    }
    dir.strip_prefix(root)
        .unwrap_or(dir)
        .trim_start_matches('/')
        .to_string()
}

fn join_uri(base: &str, relative: &str) -> String {
    format!("{}/{relative}", base.trim_end_matches('/'))
}

fn uri_name(uri: &str) -> Option<String> {
    uri.trim_end_matches('/')
        .rsplit(['/', ':'])
        .find(|part| !part.is_empty())
        .map(str::to_string)
}

fn is_local(uri: &str) -> bool {
    !uri.contains("://") || uri.starts_with("file://")
}

fn local_path(uri: &str) -> Result<std::path::PathBuf, Error> {
    if uri.starts_with("file://") {
        return url::Url::parse(uri)
            .ok()
            .and_then(|url| url.to_file_path().ok())
            .ok_or_else(|| Error("invalid local lake URI".into()));
    }
    Ok(std::path::PathBuf::from(uri))
}

fn finish(
    name: Option<String>,
    mut tables: Vec<LakeTable>,
    root: impl std::fmt::Display,
) -> Result<Lake, Error> {
    if tables.is_empty() {
        return Err(Error(format!("no tables under {root}")));
    }
    tables.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(Lake {
        kind: "pqbench.lake".into(),
        version: 1,
        name,
        tables,
    })
}

/// Serialize the lake document as pretty-printed JSON.
///
/// # Errors
/// Returns an error if the document cannot be serialized.
pub fn render_json(lake: &Lake) -> Result<String, Error> {
    serde_json::to_string_pretty(lake).map_err(|e| Error(format!("cannot serialize lake: {e}")))
}

/// Render the lake name and its tables.
pub fn render_text(lake: &Lake) -> String {
    let mut out = format!(
        "lake: {}\ntables: {}\n",
        lake.name.as_deref().unwrap_or("-"),
        lake.tables.len()
    );
    for table in &lake.tables {
        match &table.info {
            Some(info) => out.push_str(&format!(
                "  {}  snapshot {}  {} files  {}\n",
                table.name,
                info.snapshot_version,
                info.files.len(),
                table.uri
            )),
            None => out.push_str(&format!("  {}  {}\n", table.name, table.uri)),
        }
    }
    out
}
