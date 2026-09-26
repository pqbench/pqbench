//! Object access: the isolated surface. The private `impl` module is the only
//! file that names the `object_store` crate.
//!
//! [`ObjectReader`] exposes exactly what a footer read needs: the object size,
//! its identity (for optimistic concurrency), and a bounded byte-range read.
//! The factory [`open`] dispatches on the URI scheme; a scheme whose backend is
//! not compiled in fails at runtime with a message naming the missing feature.

use std::future::Future;
use std::ops::Range;
use std::path::{Path, PathBuf};
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
    /// Storage class or tier (`STANDARD_IA`, `GLACIER`, …), when HEAD reports
    /// one. S3 omits the header for `STANDARD`.
    pub storage_class: Option<String>,
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
                    storage_class: None,
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
        let is_directory_symlink = entry.path().is_symlink() && entry.path().is_dir();
        if is_directory_symlink {
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

/// Bytes fetched per remote read when copying an object to disk.
const COPY_CHUNK: u64 = 8 * 1024 * 1024;

/// Copy the object at `uri` to local `dest`, returning the bytes written.
///
/// A local path or `file://` URI is copied on the filesystem; a remote URI is
/// fetched in [`COPY_CHUNK`] pieces and streamed to disk. Parent directories of
/// `dest` are created. Backend options are passed through as `(key, value)`
/// pairs.
///
/// # Errors
/// Fails for an unsupported URI scheme, an unreadable object, or a local write
/// error.
pub async fn copy(uri: &str, dest: &Path, options: &[(String, String)]) -> Result<u64, Error> {
    if let Some(parent) = dest.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|error| Error(format!("cannot create {}: {error}", parent.display())))?;
    }
    if !is_remote(uri) {
        let source = local_path(uri);
        return tokio::fs::copy(&source, dest)
            .await
            .map_err(|error| Error(format!("cannot copy {}: {error}", source.display())));
    }
    let url =
        Url::parse(uri).map_err(|error| Error(format!("invalid object URI {uri}: {error}")))?;
    match url.scheme() {
        "s3" | "s3a" => copy_remote(uri, dest, options).await,
        scheme => Err(Error(format!(
            "unsupported object URI scheme `{scheme}`; supported schemes are file and s3"
        ))),
    }
}

async fn copy_remote(uri: &str, dest: &Path, options: &[(String, String)]) -> Result<u64, Error> {
    use tokio::io::AsyncWriteExt;

    let reader = open(uri, options)?;
    let stat = reader.stat().await?;
    let size = stat.size_bytes;
    let identity = stat.identity.as_deref();
    let mut file = tokio::fs::File::create(dest)
        .await
        .map_err(|error| Error(format!("cannot write {}: {error}", dest.display())))?;
    let mut offset = 0;
    while offset < size {
        let end = (offset + COPY_CHUNK).min(size);
        let bytes = reader.read_range(offset..end, identity).await?;
        file.write_all(&bytes)
            .await
            .map_err(|error| Error(format!("cannot write {}: {error}", dest.display())))?;
        offset = end;
    }
    Ok(size)
}

fn is_remote(uri: &str) -> bool {
    uri.contains("://") && !uri.starts_with("file://")
}

fn local_path(uri: &str) -> PathBuf {
    match uri.strip_prefix("file://") {
        Some(path) => PathBuf::from(path),
        None => PathBuf::from(uri),
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
