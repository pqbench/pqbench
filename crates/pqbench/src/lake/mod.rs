//! A lake is a list of tables. Discovery names them; it does not read logs
//! or Parquet footers. `pqbench table` loads each table, and `pqbench bytemass`
//! measures the files.

use std::collections::BTreeMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::table::TableInfo;

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

/// Find every Delta table under `root`. A directory that contains `_delta_log`
/// is a table, and its children are not searched.
///
/// # Errors
/// Fails when `root` cannot be opened or it contains no Delta tables.
pub fn discover(root: &Path) -> Result<Lake, Error> {
    let root = root
        .canonicalize()
        .map_err(|e| Error(format!("cannot open lake {}: {e}", root.display())))?;
    if !root.is_dir() {
        return Err(Error(format!("{} is not a directory", root.display())));
    }
    let mut tables = Vec::new();
    walk(&root, &root, &mut tables)?;
    if tables.is_empty() {
        return Err(Error(format!("no delta tables under {}", root.display())));
    }
    tables.sort_by(|left, right| left.name.cmp(&right.name));
    let name = root
        .file_name()
        .map(|name| name.to_string_lossy().into_owned());
    Ok(Lake {
        kind: "pqbench.lake".into(),
        version: 1,
        name,
        tables,
    })
}

fn walk(root: &Path, dir: &Path, tables: &mut Vec<LakeTable>) -> Result<(), Error> {
    if dir.join("_delta_log").is_dir() {
        let name = if dir == root {
            dir.file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_else(|| "table".into())
        } else {
            dir.strip_prefix(root)
                .map_err(|e| Error(e.to_string()))?
                .to_string_lossy()
                .replace('\\', "/")
        };
        tables.push(LakeTable {
            name,
            uri: dir.to_string_lossy().into_owned(),
            env: BTreeMap::new(),
            info: None,
        });
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
        walk(root, &path, tables)?;
    }
    Ok(())
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
