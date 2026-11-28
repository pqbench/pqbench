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
#[non_exhaustive]
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
    /// The object is in an archive class or tier and needs a restore first.
    Archived {
        /// The object's URI.
        uri: String,
        /// The storage class or archive tier HEAD reported.
        storage_class: Option<String>,
        /// Whether a restore is running but has not finished.
        restore_ongoing: bool,
    },
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
            Error::Archived {
                uri,
                storage_class,
                restore_ongoing,
            } => {
                let class = storage_class.as_deref().unwrap_or("an archive tier");
                if *restore_ongoing {
                    write!(
                        f,
                        "object {uri} is in {class}; its restore is still running, so it cannot be read yet"
                    )
                } else {
                    write!(
                        f,
                        "object {uri} is in {class} and must be restored before it can be read"
                    )
                }
            }
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
            | Error::Invalid(_)
            | Error::Archived { .. } => None,
        }
    }
}

/// What one object lookup reports before any bytes are read.
#[derive(Debug, Clone, Default)]
pub struct ObjectStat {
    /// Object size in bytes.
    pub size_bytes: u64,
    /// Backend ETag or version, used to pin range reads to one revision.
    pub identity: Option<String>,
    /// Storage class or tier (`STANDARD_IA`, `GLACIER`, …), when HEAD reports
    /// one. S3 omits the header for `STANDARD`.
    pub storage_class: Option<String>,
    /// The Intelligent-Tiering archive tier HEAD reports
    /// (`x-amz-archive-status`): `ARCHIVE_ACCESS` or `DEEP_ARCHIVE_ACCESS`.
    pub archive_status: Option<String>,
    /// Whether `x-amz-restore` reports a restore still in progress.
    pub restore_ongoing: bool,
    /// When a completed restore ends, milliseconds since the Unix epoch.
    pub restore_expiry_time: Option<u64>,
    /// Whether `x-amz-restore` reported `ongoing-request="false"`, even when it
    /// named no expiry.
    pub restore_completed: bool,
}

impl ObjectStat {
    /// Whether reading this object needs a restore first, for `service`.
    ///
    /// S3 archives an object in `GLACIER` or `DEEP_ARCHIVE`, or in an
    /// Intelligent-Tiering archive access tier; a completed restore makes it
    /// readable again, while `GLACIER_IR` is always readable. GCS `ARCHIVE` is
    /// online (millisecond retrieval), so it never needs a restore. An unknown
    /// or absent class fails open: the storage read decides.
    #[must_use]
    pub fn restore_required(&self, service: &str) -> bool {
        match service {
            "s3" | "s3a" => self.s3_restore_required(),
            _ => false,
        }
    }

    fn s3_restore_required(&self) -> bool {
        let archived_class = self
            .storage_class
            .as_deref()
            .is_some_and(|class| matches!(class, "GLACIER" | "DEEP_ARCHIVE"));
        let archive_tier = self
            .archive_status
            .as_deref()
            .is_some_and(|tier| matches!(tier, "ARCHIVE_ACCESS" | "DEEP_ARCHIVE_ACCESS"));
        if !archived_class && !archive_tier {
            return false;
        }
        if self.restore_ongoing {
            return true;
        }
        !(self.restore_completed || self.restore_expiry_time.is_some())
    }
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
                    archive_status: None,
                    restore_ongoing: false,
                    restore_expiry_time: None,
                    restore_completed: false,
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
/// URIs require the `aws` feature. The lake's options configure the backend.
pub async fn list_prefix(lake: &Lake, uri: &str) -> Result<PrefixListing, Error> {
    let url = Url::parse(uri).map_err(|e| Error::Uri {
        uri: uri.to_string(),
        source: e,
    })?;
    match url.scheme() {
        "file" => list_file(&url),
        "s3" | "s3a" => lake.stores().list_remote(&url, lake.options()).await,
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
/// `file` URIs always work; `s3`/`s3a` URIs require the `aws` feature. The
/// lake's options configure the backend.
pub fn open(lake: &Lake, uri: &str) -> Result<ObjectReader, Error> {
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
        "s3" | "s3a" => lake.stores().open_remote(&url, lake.options()),
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
/// `dest` are created. The lake's options configure the backend.
///
/// # Errors
/// Fails for an unsupported URI scheme, an unreadable object, or a local write
/// error.
pub async fn copy(lake: &Lake, uri: &str, dest: &Path) -> Result<u64, Error> {
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
        "s3" | "s3a" => copy_remote(lake, uri, dest).await,
        scheme => Err(Error::Unsupported(format!(
            "unsupported object URI scheme `{scheme}`; supported schemes are file and s3"
        ))),
    }
}

async fn copy_remote(lake: &Lake, uri: &str, dest: &Path) -> Result<u64, Error> {
    use std::io::Write;

    let reader = open(lake, uri)?;
    let stat = reader.stat().await?;
    let service = uri.split("://").next().unwrap_or_default();
    if stat.restore_required(service) {
        return Err(Error::Archived {
            uri: uri.to_string(),
            storage_class: stat
                .storage_class
                .clone()
                .or_else(|| stat.archive_status.clone()),
            restore_ongoing: stat.restore_ongoing,
        });
    }
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
pub async fn expand_glob(lake: &Lake, uri: &str) -> Result<Vec<String>, Error> {
    if !uri.contains(['*', '?']) {
        return Ok(vec![uri.to_owned()]);
    }
    lake.stores().expand_glob(uri, lake.options()).await
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

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::ops::Range;
    use std::pin::Pin;

    use super::{Error, ObjectReader, ObjectStat, Remote};

    /// A remote whose HEAD answers one fixed stat.
    struct FakeRemote(ObjectStat);

    impl Remote for FakeRemote {
        fn exists<'a>(&'a self) -> Pin<Box<dyn Future<Output = Result<bool, Error>> + Send + 'a>> {
            Box::pin(async { Ok(true) })
        }

        fn stat<'a>(
            &'a self,
        ) -> Pin<Box<dyn Future<Output = Result<ObjectStat, Error>> + Send + 'a>> {
            Box::pin(async { Ok(self.0.clone()) })
        }

        fn read_range<'a>(
            &'a self,
            _range: Range<u64>,
            _identity: Option<String>,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, Error>> + Send + 'a>> {
            Box::pin(async { Ok(Vec::new()) })
        }
    }

    fn stat(class: &str, archive_status: Option<&str>) -> ObjectStat {
        ObjectStat {
            storage_class: Some(class.to_string()),
            archive_status: archive_status.map(str::to_string),
            ..ObjectStat::default()
        }
    }

    /// S3 blocks Glacier and Deep Archive, and an Intelligent-Tiering archive
    /// access tier, but never Glacier Instant Retrieval: its bytes are online.
    #[test]
    fn s3_blocks_archive_classes_and_tiers_only() {
        assert!(stat("GLACIER", None).restore_required("s3"));
        assert!(stat("DEEP_ARCHIVE", None).restore_required("s3"));
        assert!(stat("INTELLIGENT_TIERING", Some("ARCHIVE_ACCESS")).restore_required("s3"));
        assert!(stat("INTELLIGENT_TIERING", Some("DEEP_ARCHIVE_ACCESS")).restore_required("s3"));
        assert!(!stat("GLACIER_IR", None).restore_required("s3"));
        assert!(!stat("INTELLIGENT_TIERING", Some("ARCHIVE_INSTANT_ACCESS")).restore_required("s3"));
        assert!(!stat("STANDARD", None).restore_required("s3"));
        assert!(!ObjectStat::default().restore_required("s3"));
    }

    /// A completed restore makes an archive object readable; an in-progress
    /// one leaves it blocked.
    #[test]
    fn a_completed_restore_unblocks_an_archive_object() {
        let mut restored = stat("GLACIER", None);
        restored.restore_completed = true;
        assert!(!restored.restore_required("s3"));

        let mut expiring = stat("DEEP_ARCHIVE", None);
        expiring.restore_expiry_time = Some(1_700_000_000_000);
        assert!(!expiring.restore_required("s3"));

        let mut ongoing = stat("GLACIER", None);
        ongoing.restore_ongoing = true;
        assert!(ongoing.restore_required("s3"));
    }

    /// GCS Archive is online (millisecond retrieval), so it is never blocked,
    /// and an unknown class fails open.
    #[test]
    fn gcs_archive_and_unknown_classes_are_readable() {
        assert!(!stat("ARCHIVE", None).restore_required("gs"));
        assert!(!stat("ARCHIVE", None).restore_required("gcs"));
        assert!(!stat("SOMETHING_NEW", None).restore_required("s3"));
    }

    /// The stat an `ObjectReader` reports is what the predicate sees: a fake
    /// remote's Glacier object is an archive read, and its Glacier IR one is
    /// not.
    #[tokio::test]
    async fn an_object_reader_reports_the_archive_fact() {
        let glacier = ObjectReader::from_remote(Box::new(FakeRemote(stat("GLACIER", None))));
        assert!(glacier.stat().await.unwrap().restore_required("s3"));
        let instant = ObjectReader::from_remote(Box::new(FakeRemote(stat("GLACIER_IR", None))));
        assert!(!instant.stat().await.unwrap().restore_required("s3"));
    }

    #[test]
    fn an_archive_error_names_the_class_and_the_restore() {
        let error = Error::Archived {
            uri: "s3://bucket/key.parquet".to_string(),
            storage_class: Some("GLACIER".to_string()),
            restore_ongoing: false,
        };
        let message = error.to_string();
        assert!(message.contains("GLACIER"), "{message}");
        assert!(message.contains("restored"), "{message}");
    }
}
