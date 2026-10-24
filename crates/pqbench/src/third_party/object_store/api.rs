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

use crate::Lake;

/// Errors from the object-storage layer.
#[derive(Debug)]
pub enum Error {
    /// A URI could not be parsed.
    Uri {
        /// The URI that failed to parse.
        uri: String,
        /// The underlying parse failure.
        source: url::ParseError,
    },
    /// A local path could not be turned into a file URI.
    LocalPath(String),
    /// The operation is not supported by this build or URI scheme.
    Unsupported(String),
    /// The store registry lock was poisoned.
    Poisoned,
    /// A key pattern was invalid.
    Pattern {
        /// The mask that failed to parse.
        mask: String,
        /// The underlying glob failure.
        source: glob::PatternError,
    },
    /// The glob matched no objects.
    NoMatch(String),
    /// An object-store backend call failed.
    Store(Box<dyn std::error::Error + Send + Sync>),
    /// A local filesystem call failed.
    Io {
        /// What the call was doing (`"cannot stat"`, `"cannot list"`, …).
        context: &'static str,
        /// The path involved.
        path: String,
        /// The underlying I/O failure.
        source: std::io::Error,
    },
    /// The transport to the store failed.
    Transport(crate::third_party::reqwest::Error),
    /// A store option name was not recognized.
    Option(String),
    /// A response was missing a required field.
    Invalid(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Uri { uri, .. } => write!(f, "invalid object URI {uri}"),
            Error::LocalPath(uri) => write!(f, "invalid local file URI: {uri}"),
            Error::Unsupported(message) => write!(f, "{message}"),
            Error::Poisoned => write!(f, "the store registry is poisoned"),
            Error::Pattern { mask, .. } => write!(f, "invalid key glob {mask}"),
            Error::NoMatch(mask) => write!(f, "the glob matched no files: {mask}"),
            Error::Store(_) => write!(f, "object storage failed"),
            Error::Io { context, path, .. } => write!(f, "{context} {path}"),
            Error::Transport(_) => write!(f, "the object-store transport failed"),
            Error::Option(key) => write!(f, "unknown object store option `{key}`"),
            Error::Invalid(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Uri { source, .. } => Some(source),
            Error::Pattern { source, .. } => Some(source),
            Error::Store(source) => Some(source.as_ref()),
            Error::Io { source, .. } => Some(source),
            Error::Transport(source) => Some(source),
            Error::LocalPath(_)
            | Error::Unsupported(_)
            | Error::Poisoned
            | Error::NoMatch(_)
            | Error::Option(_)
            | Error::Invalid(_) => None,
        }
    }
}

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
            ObjectSource::Local(path) => Ok(std::fs::metadata(path).is_ok()),
            ObjectSource::Remote(remote) => remote.exists().await,
        }
    }

    /// Report the object's size and identity.
    pub async fn stat(&self) -> Result<ObjectStat, Error> {
        match &self.source {
            ObjectSource::Local(path) => {
                let metadata = std::fs::metadata(path).map_err(|e| Error::Io {
                    context: "cannot stat",
                    path: path.display().to_string(),
                    source: e,
                })?;
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
            ObjectSource::Local(path) => read_local(path, range),
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
pub async fn list_prefix(
    lake: &Lake,
    uri: &str,
    options: &[(String, String)],
) -> Result<PrefixListing, Error> {
    let url = Url::parse(uri).map_err(|e| Error::Uri {
        uri: uri.to_string(),
        source: e,
    })?;
    match url.scheme() {
        "file" => list_file(&url),
        "s3" | "s3a" => lake.stores().list_remote(&url, options).await,
        scheme => Err(Error::Unsupported(format!(
            "listing is not supported for object URI scheme `{scheme}`"
        ))),
    }
}

fn list_file(url: &Url) -> Result<PrefixListing, Error> {
    let path = url
        .to_file_path()
        .map_err(|()| Error::LocalPath(url.to_string()))?;
    if !path.is_dir() {
        return Ok(PrefixListing {
            prefixes: Vec::new(),
            objects: Vec::new(),
        });
    }
    let entries = std::fs::read_dir(&path).map_err(|e| Error::Io {
        context: "cannot list",
        path: path.display().to_string(),
        source: e,
    })?;
    let mut prefixes = Vec::new();
    let mut objects = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|e| Error::Io {
            context: "cannot list",
            path: path.display().to_string(),
            source: e,
        })?;
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
pub fn open(lake: &Lake, uri: &str, options: &[(String, String)]) -> Result<ObjectReader, Error> {
    let url = Url::parse(uri).map_err(|e| Error::Uri {
        uri: uri.to_string(),
        source: e,
    })?;
    match url.scheme() {
        "file" => {
            let path = url
                .to_file_path()
                .map_err(|()| Error::LocalPath(uri.to_string()))?;
            Ok(ObjectReader {
                source: ObjectSource::Local(path),
            })
        }
        "s3" | "s3a" => lake.stores().open_remote(&url, options),
        scheme => Err(Error::Unsupported(format!(
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
pub async fn copy(
    lake: &Lake,
    uri: &str,
    dest: &Path,
    options: &[(String, String)],
) -> Result<u64, Error> {
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent).map_err(|error| Error::Io {
            context: "cannot create",
            path: parent.display().to_string(),
            source: error,
        })?;
    }
    if !is_remote(uri) {
        let source = local_path(uri);
        return std::fs::copy(&source, dest).map_err(|error| Error::Io {
            context: "cannot copy",
            path: source.display().to_string(),
            source: error,
        });
    }
    let url = Url::parse(uri).map_err(|error| Error::Uri {
        uri: uri.to_string(),
        source: error,
    })?;
    match url.scheme() {
        "s3" | "s3a" => copy_remote(lake, uri, dest, options).await,
        scheme => Err(Error::Unsupported(format!(
            "unsupported object URI scheme `{scheme}`; supported schemes are file and s3"
        ))),
    }
}

async fn copy_remote(
    lake: &Lake,
    uri: &str,
    dest: &Path,
    options: &[(String, String)],
) -> Result<u64, Error> {
    use std::io::Write;

    let reader = open(lake, uri, options)?;
    let stat = reader.stat().await?;
    let size = stat.size_bytes;
    let identity = stat.identity.as_deref();
    let mut file = std::fs::File::create(dest).map_err(|error| Error::Io {
        context: "cannot write",
        path: dest.display().to_string(),
        source: error,
    })?;
    let mut offset = 0;
    while offset < size {
        let end = (offset + COPY_CHUNK).min(size);
        let bytes = reader.read_range(offset..end, identity).await?;
        file.write_all(&bytes).map_err(|error| Error::Io {
            context: "cannot write",
            path: dest.display().to_string(),
            source: error,
        })?;
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

fn read_local(path: &std::path::Path, range: Range<u64>) -> Result<Vec<u8>, Error> {
    use std::io::{Read, Seek, SeekFrom};

    let mut file = std::fs::File::open(path).map_err(|e| Error::Io {
        context: "cannot open",
        path: path.display().to_string(),
        source: e,
    })?;
    file.seek(SeekFrom::Start(range.start))
        .map_err(|e| Error::Io {
            context: "cannot seek",
            path: path.display().to_string(),
            source: e,
        })?;
    let length = usize::try_from(range.end.saturating_sub(range.start))
        .map_err(|_| Error::Invalid(format!("range too large for {}", path.display())))?;
    let mut buffer = vec![0; length];
    file.read_exact(&mut buffer).map_err(|e| Error::Io {
        context: "cannot read",
        path: path.display().to_string(),
        source: e,
    })?;
    Ok(buffer)
}

/// Expand an S3 key glob, listing only under its literal directory prefix.
///
/// `*` and `?` match within a component; `**` traverses directories. Percent
/// escapes address literal key characters. Exact URIs do not require listing.
///
/// # Errors
/// Invalid/unsupported URIs, disabled backend, listing failure, or no matches.
pub async fn expand_glob(
    lake: &Lake,
    uri: &str,
    options: &[(String, String)],
) -> Result<Vec<String>, Error> {
    if !uri.contains(['*', '?']) {
        return Ok(vec![uri.to_owned()]);
    }
    lake.stores().expand_glob(uri, options).await
}

/// A key glob with a literal listing prefix and component-aware matching.
pub struct KeyPattern {
    pattern: glob::Pattern,
    prefix: String,
}
impl KeyPattern {
    /// Parse a key pattern; square brackets are literal, as in local inputs.
    ///
    /// # Errors
    /// Missing wildcards or invalid glob syntax.
    pub fn parse(mask: &str) -> Result<Self, Error> {
        let wildcard = mask
            .find(['*', '?'])
            .ok_or_else(|| Error::Invalid("missing key wildcard".into()))?;
        let prefix = mask[..wildcard]
            .rfind('/')
            .map_or("", |end| &mask[..end])
            .to_owned();
        let pattern =
            glob::Pattern::new(&mask.replace('[', "[[]")).map_err(|e| Error::Pattern {
                mask: mask.to_string(),
                source: e,
            })?;
        Ok(Self { pattern, prefix })
    }
    /// Literal directory prefix to list, empty for the bucket root.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }
    /// Match a decoded object key; ordinary wildcards cannot cross slashes.
    pub fn matches(&self, key: &str) -> bool {
        self.pattern.matches_with(
            key,
            glob::MatchOptions {
                require_literal_separator: true,
                ..Default::default()
            },
        )
    }
}
