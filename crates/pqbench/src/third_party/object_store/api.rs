//! Object access: the isolated surface. The private `impl` module is the only
//! file that names the `object_store` crate.
//!
//! [`ObjectReader`] exposes exactly what a footer read needs: the object size,
//! its identity (for optimistic concurrency), and a bounded byte-range read.
//! The factory [`open`] dispatches on the URI scheme; a scheme whose backend is
//! not compiled in fails at runtime with a message naming the missing feature.

use std::future::Future;
use std::ops::Range;
use std::path::PathBuf;
use std::pin::Pin;

use url::Url;

/// Errors from the object-storage layer.
#[derive(Debug)]
pub struct Error(pub String);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "object store: {}", self.0)
    }
}

impl std::error::Error for Error {}

/// What one object lookup reports before any bytes are read.
pub struct ObjectStat {
    /// Object size in bytes.
    pub size_bytes: u64,
    /// Backend ETag or version, used to pin range reads to one revision.
    pub identity: Option<String>,
}

/// A remote backend. Implemented by the private `impl` module over the
/// third-party store; the trait names no third-party type.
///
/// Unused until a backend is compiled in; kept here so the api never sees a
/// feature flag.
#[allow(dead_code)]
pub trait Remote: Send + Sync {
    fn exists<'a>(&'a self) -> Pin<Box<dyn Future<Output = Result<bool, Error>> + Send + 'a>>;
    fn stat<'a>(&'a self) -> Pin<Box<dyn Future<Output = Result<ObjectStat, Error>> + Send + 'a>>;
    fn read_range<'a>(
        &'a self,
        range: Range<u64>,
        identity: Option<String>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, Error>> + Send + 'a>>;
}

/// A random-access handle to a single object (local or remote).
pub struct ObjectReader {
    source: ObjectSource,
}

enum ObjectSource {
    Local(PathBuf),
    Remote(Box<dyn Remote>),
}

impl ObjectReader {
    /// Wrap a remote backend. Called by the private `impl` module.
    #[allow(dead_code)]
    pub fn from_remote(remote: Box<dyn Remote>) -> Self {
        Self {
            source: ObjectSource::Remote(remote),
        }
    }

    /// Whether the object exists. A missing object is `Ok(false)`.
    pub async fn exists(&self) -> Result<bool, Error> {
        match &self.source {
            ObjectSource::Local(path) => Ok(tokio::fs::metadata(path).await.is_ok()),
            ObjectSource::Remote(remote) => remote.exists().await,
        }
    }

    /// Report the object's size and identity.
    pub async fn stat(&self) -> Result<ObjectStat, Error> {
        match &self.source {
            ObjectSource::Local(path) => {
                let metadata = tokio::fs::metadata(path)
                    .await
                    .map_err(|e| Error(format!("cannot stat {}: {e}", path.display())))?;
                Ok(ObjectStat {
                    size_bytes: metadata.len(),
                    identity: None,
                })
            }
            ObjectSource::Remote(remote) => remote.stat().await,
        }
    }

    /// Read a bounded byte range, optionally pinned to a known identity.
    pub async fn read_range(
        &self,
        range: Range<u64>,
        identity: Option<&str>,
    ) -> Result<Vec<u8>, Error> {
        match &self.source {
            ObjectSource::Local(path) => read_local(path, range).await,
            ObjectSource::Remote(remote) => {
                remote.read_range(range, identity.map(str::to_owned)).await
            }
        }
    }
}

/// One level of names under a prefix. Listing does not recurse.
pub struct PrefixListing {
    /// Child prefix names. A table walk stops when one of these is a marker.
    pub prefixes: Vec<String>,
    /// Object names at this level (one path component).
    pub objects: Vec<String>,
}

/// List one level of children under `uri`. `file` URIs always work; `s3`/`s3a`
/// URIs require the `aws` feature. Backend options are passed through as
/// `(key, value)` pairs.
pub async fn list_prefix(uri: &str, options: &[(String, String)]) -> Result<PrefixListing, Error> {
    let url = Url::parse(uri).map_err(|e| Error(format!("invalid object URI {uri}: {e}")))?;
    match url.scheme() {
        "file" => list_file(&url),
        "s3" | "s3a" => super::r#impl::list_remote(&url, options).await,
        scheme => Err(Error(format!(
            "listing is not supported for object URI scheme `{scheme}`"
        ))),
    }
}

fn list_file(url: &Url) -> Result<PrefixListing, Error> {
    let path = url
        .to_file_path()
        .map_err(|()| Error(format!("invalid local file URI: {url}")))?;
    if !path.is_dir() {
        return Ok(PrefixListing {
            prefixes: Vec::new(),
            objects: Vec::new(),
        });
    }
    let entries = std::fs::read_dir(&path)
        .map_err(|e| Error(format!("cannot list {}: {e}", path.display())))?;
    let mut prefixes = Vec::new();
    let mut objects = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|e| Error(format!("cannot list {}: {e}", path.display())))?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with('.') {
            continue;
        }
        // A symlinked directory is not walked, so a listing cannot loop.
        if entry.path().is_symlink() && entry.path().is_dir() {
            continue;
        }
        if entry.path().is_dir() {
            prefixes.push(name);
        } else {
            objects.push(name);
        }
    }
    Ok(PrefixListing { prefixes, objects })
}

/// Open the object named by `uri`.
///
/// `file` URIs always work; `s3`/`s3a` URIs require the `aws` feature. Backend
/// options are passed through as `(key, value)` pairs.
pub fn open(uri: &str, options: &[(String, String)]) -> Result<ObjectReader, Error> {
    let url = Url::parse(uri).map_err(|e| Error(format!("invalid object URI {uri}: {e}")))?;
    match url.scheme() {
        "file" => {
            let path = url
                .to_file_path()
                .map_err(|()| Error(format!("invalid local file URI: {uri}")))?;
            Ok(ObjectReader {
                source: ObjectSource::Local(path),
            })
        }
        "s3" | "s3a" => super::r#impl::open_remote(&url, options),
        scheme => Err(Error(format!(
            "unsupported object URI scheme `{scheme}`; supported schemes are file and s3"
        ))),
    }
}

async fn read_local(path: &std::path::Path, range: Range<u64>) -> Result<Vec<u8>, Error> {
    use std::io::SeekFrom;

    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|e| Error(format!("cannot open {}: {e}", path.display())))?;
    file.seek(SeekFrom::Start(range.start))
        .await
        .map_err(|e| Error(format!("cannot seek {}: {e}", path.display())))?;
    let length = usize::try_from(range.end.saturating_sub(range.start))
        .map_err(|_| Error(format!("range too large for {}", path.display())))?;
    let mut buffer = vec![0; length];
    file.read_exact(&mut buffer)
        .await
        .map_err(|e| Error(format!("cannot read {}: {e}", path.display())))?;
    Ok(buffer)
}
