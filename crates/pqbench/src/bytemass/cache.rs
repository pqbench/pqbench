//! Persistent file-mass cache for Parquet footer measurements.
//!
//! The cache is keyed by object identity (URI, size, and optional S3 ETag),
//! not by table snapshot. An unchanged object is reused; a new ETag or size
//! is a miss. Delta snapshots benefit because they measure the same objects.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::parquet_helpers::{ColumnMass, Error, FileMass};

const KIND: &str = "pqbench.file-mass";
const VERSION: u32 = 1;

/// Local directory of file-mass documents, with an in-process layer in front.
#[derive(Clone)]
pub struct FileMassCache {
    directory: PathBuf,
    memory: Arc<Mutex<HashMap<CacheKey, FileMass>>>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct CacheKey {
    uri: String,
    size: u64,
    identity: Option<String>,
}

#[derive(Deserialize, Serialize)]
struct FileMassDocument {
    kind: String,
    version: u32,
    uri: String,
    size: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    identity: Option<String>,
    num_rows: u64,
    columns: Vec<DocumentColumn>,
}

#[derive(Deserialize, Serialize)]
struct DocumentColumn {
    path: String,
    compressed_bytes: u64,
    uncompressed_bytes: u64,
    codec: String,
}

impl FileMassCache {
    /// Open (and create) a cache directory.
    ///
    /// # Errors
    /// Fails if the directory cannot be created.
    pub fn open(directory: impl Into<PathBuf>) -> Result<Self, Error> {
        let directory = directory.into();
        fs::create_dir_all(&directory).map_err(|e| {
            Error(format!(
                "cannot create file-mass cache {}: {e}",
                directory.display()
            ))
        })?;
        Ok(Self {
            directory,
            memory: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Return a previously stored mass for this object identity.
    ///
    /// # Errors
    /// Fails on unreadable cache files or a poisoned in-process lock.
    pub fn get(
        &self,
        uri: &str,
        size: u64,
        identity: Option<&str>,
    ) -> Result<Option<FileMass>, Error> {
        let key = CacheKey::new(uri, size, identity);
        if let Some(mass) = self.memory()?.get(&key).cloned() {
            return Ok(Some(mass));
        }
        let path = self.document_path(&key);
        let document = match fs::read_to_string(&path) {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(Error(format!(
                    "cannot read file-mass cache {}: {error}",
                    path.display()
                )));
            }
        };
        let Some(mass) = parse_document(&document, &key) else {
            return Ok(None);
        };
        self.memory()?.insert(key, mass.clone());
        Ok(Some(mass))
    }

    /// Store a measured mass under this object identity.
    ///
    /// # Errors
    /// Fails if the document cannot be written or the in-process lock is poisoned.
    pub fn put(
        &self,
        uri: &str,
        size: u64,
        identity: Option<&str>,
        mass: &FileMass,
    ) -> Result<(), Error> {
        let key = CacheKey::new(uri, size, identity);
        let path = self.document_path(&key);
        let document = FileMassDocument {
            kind: KIND.into(),
            version: VERSION,
            uri: uri.to_owned(),
            size,
            identity: identity.map(str::to_owned),
            num_rows: mass.num_rows,
            columns: mass
                .columns
                .iter()
                .map(|column| DocumentColumn {
                    path: column.path.clone(),
                    compressed_bytes: column.bytes,
                    uncompressed_bytes: column.uncompressed_bytes,
                    codec: column.codec.clone(),
                })
                .collect(),
        };
        let text = serde_json::to_string_pretty(&document)
            .map_err(|e| Error(format!("cannot serialize file-mass cache: {e}")))?;
        write_atomic(&path, text.as_bytes())?;
        self.memory()?.insert(key, mass.clone());
        Ok(())
    }

    fn document_path(&self, key: &CacheKey) -> PathBuf {
        self.directory.join(format!("{}.json", key.digest()))
    }

    fn memory(&self) -> Result<std::sync::MutexGuard<'_, HashMap<CacheKey, FileMass>>, Error> {
        self.memory
            .lock()
            .map_err(|_| Error("file-mass cache lock poisoned".into()))
    }
}

impl CacheKey {
    fn new(uri: &str, size: u64, identity: Option<&str>) -> Self {
        Self {
            uri: uri.to_owned(),
            size,
            identity: identity.map(str::to_owned),
        }
    }

    fn digest(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(self.uri.as_bytes());
        hasher.update([0]);
        hasher.update(self.size.to_le_bytes());
        hasher.update([0]);
        if let Some(identity) = &self.identity {
            hasher.update(identity.as_bytes());
        }
        hex_digest(&hasher.finalize())
    }
}

fn hex_digest(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut digest = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        digest.push(HEX[(byte >> 4) as usize] as char);
        digest.push(HEX[(byte & 0x0f) as usize] as char);
    }
    digest
}

fn parse_document(text: &str, key: &CacheKey) -> Option<FileMass> {
    let document: FileMassDocument = serde_json::from_str(text).ok()?;
    if document.kind != KIND
        || document.version != VERSION
        || document.uri != key.uri
        || document.size != key.size
        || document.identity != key.identity
    {
        return None;
    }
    Some(FileMass {
        num_rows: document.num_rows,
        columns: document
            .columns
            .into_iter()
            .map(|column| ColumnMass {
                path: column.path,
                bytes: column.compressed_bytes,
                uncompressed_bytes: column.uncompressed_bytes,
                codec: column.codec,
            })
            .collect(),
    })
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), Error> {
    let temporary = path.with_extension("json.tmp");
    fs::write(&temporary, bytes).map_err(|e| {
        Error(format!(
            "cannot write file-mass cache {}: {e}",
            temporary.display()
        ))
    })?;
    fs::rename(&temporary, path).map_err(|e| {
        Error(format!(
            "cannot commit file-mass cache {}: {e}",
            path.display()
        ))
    })
}
