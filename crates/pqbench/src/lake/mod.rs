//! A lake is a list of tables. Discovery names them; it does not read logs
//! or Parquet footers. `pqbench table` loads each table, and `pqbench bytemass`
//! measures the files.

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::table::TableInfo;
use crate::third_party::object_store::{self, PrefixListing};

/// Directory whose presence marks a Delta table.
const DELTA_LOG: &str = "_delta_log";
/// Directory that holds Iceberg metadata.
const ICEBERG_METADATA: &str = "metadata";
/// Iceberg version file inside [`ICEBERG_METADATA`].
const ICEBERG_VERSION_HINT: &str = "version-hint.text";
const ICEBERG_METADATA_SUFFIX: &str = ".metadata.json";

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

/// Find every Delta or Iceberg table under `uri`. A bare path or `file://`
/// URI lists the filesystem; `s3://` (with the `aws` feature) lists object
/// storage. A directory that contains `_delta_log`,
/// `metadata/version-hint.text`, or `metadata/*.metadata.json` is a table, and
/// its children are not searched. Delta wins when both markers are present
/// (UniForm).
///
/// # Errors
/// Fails when the location cannot be opened, listing fails, or it contains no
/// tables.
pub async fn discover(uri: &str, env: &BTreeMap<String, String>) -> Result<Lake, Error> {
    discover_bounded(uri, env, None).await
}

/// Find every table under `uri`, stopping after `max_depth` path components
/// below the root. `None` walks until a marker.
///
/// # Errors
/// As [`discover`].
pub async fn discover_bounded(
    uri: &str,
    env: &BTreeMap<String, String>,
    max_depth: Option<usize>,
) -> Result<Lake, Error> {
    let root = root_uri(uri)?;
    let options = env_options(env);
    let mut tables = Vec::new();
    let mut pending = vec![(root.clone(), 0usize)];
    while let Some((dir, depth)) = pending.pop() {
        let listing = object_store::list_prefix(&dir, &options)
            .await
            .map_err(|e| Error(e.to_string()))?;
        if is_table(&listing, &dir, &options).await? {
            tables.push(LakeTable {
                name: table_name(&root, &dir),
                uri: dir.trim_end_matches('/').to_string(),
                env: env.clone(),
                info: None,
            });
            continue;
        }
        if max_depth.is_some_and(|max| depth >= max) {
            continue;
        }
        for prefix in listing.prefixes {
            if is_walkable_prefix(&prefix) {
                pending.push((join_uri(&dir, &prefix)?, depth + 1));
            }
        }
    }
    finish(uri_name(&root), tables, &root)
}

fn env_options(env: &BTreeMap<String, String>) -> Vec<(String, String)> {
    env.iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

/// Normalize a walk root to a URI: a bare path becomes a canonical `file://`
/// URL, an object URI keeps its scheme.
fn root_uri(uri: &str) -> Result<String, Error> {
    if !is_local(uri) {
        return Ok(uri.trim_end_matches('/').to_string());
    }
    let path = local_path(uri)?;
    let path = path
        .canonicalize()
        .map_err(|e| Error(format!("cannot open lake {}: {e}", path.display())))?;
    if !path.is_dir() {
        return Err(Error(format!("{} is not a directory", path.display())));
    }
    url::Url::from_directory_path(&path)
        .map(String::from)
        .map_err(|()| Error(format!("invalid local lake path {}", path.display())))
}

async fn is_table(
    listing: &PrefixListing,
    dir: &str,
    options: &[(String, String)],
) -> Result<bool, Error> {
    Ok(listing_is_delta(listing) || listing_is_iceberg(listing, dir, options).await?)
}

fn is_walkable_prefix(prefix: &str) -> bool {
    !prefix.is_empty()
        && !prefix.starts_with('.')
        && prefix != DELTA_LOG
        && prefix != ICEBERG_METADATA
}

fn table_name(root: &str, dir: &str) -> String {
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

fn listing_is_delta(listing: &PrefixListing) -> bool {
    listing.prefixes.iter().any(|name| name == DELTA_LOG)
}

async fn listing_is_iceberg(
    listing: &PrefixListing,
    dir: &str,
    options: &[(String, String)],
) -> Result<bool, Error> {
    if !listing.prefixes.iter().any(|name| name == ICEBERG_METADATA) {
        return Ok(false);
    }
    let metadata = object_store::list_prefix(&join_uri(dir, ICEBERG_METADATA)?, options)
        .await
        .map_err(|e| Error(e.to_string()))?;
    Ok(metadata
        .objects
        .iter()
        .any(|name| is_iceberg_metadata(name)))
}

fn is_iceberg_metadata(name: &str) -> bool {
    if name.contains('/') {
        return false;
    }
    if name == ICEBERG_VERSION_HINT {
        return true;
    }
    name.ends_with(ICEBERG_METADATA_SUFFIX) && name != ICEBERG_METADATA_SUFFIX
}

fn join_uri(base: &str, relative: &str) -> Result<String, Error> {
    let mut url =
        url::Url::parse(base).map_err(|e| Error(format!("invalid lake URI {base}: {e}")))?;
    if !url.path().ends_with('/') {
        url.set_path(&format!("{}/", url.path()));
    }
    Ok(url
        .join(relative)
        .map_err(|e| Error(format!("invalid lake path {relative}: {e}")))?
        .into())
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

fn local_path(uri: &str) -> Result<PathBuf, Error> {
    if uri.starts_with("file://") {
        return url::Url::parse(uri)
            .ok()
            .and_then(|url| url.to_file_path().ok())
            .ok_or_else(|| Error("invalid local lake URI".into()));
    }
    Ok(PathBuf::from(uri))
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
