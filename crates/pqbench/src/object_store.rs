//! Remote object access, isolated from the rest of pqbench.
//!
//! This is the only module that names the third-party `object_store` crate.
//! Everything else uses [`ObjectReader`], which exposes exactly what a footer
//! read needs: the object size, its identity (for optimistic concurrency), and
//! a bounded byte-range read.
//!
//! The S3 backend is compiled behind the `aws` feature. The factory [`open`]
//! dispatches on the URI scheme; a scheme whose backend is not compiled in
//! fails at runtime with a message naming the missing feature. There are no
//! feature flags outside this module.

use std::ops::Range;
use std::path::PathBuf;

use url::Url;

/// Errors from the object-storage layer.
#[derive(Debug)]
pub(crate) struct Error(String);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "object store: {}", self.0)
    }
}

impl std::error::Error for Error {}

/// What one object lookup reports before any bytes are read.
pub(crate) struct ObjectStat {
    /// Object size in bytes.
    pub size: u64,
    /// Backend ETag or version, used to pin range reads to one revision.
    pub identity: Option<String>,
}

/// A random-access handle to a single object (local or remote).
pub(crate) struct ObjectReader {
    source: Source,
}

enum Source {
    Local(PathBuf),
    #[cfg(feature = "aws")]
    Remote(
        Box<dyn ::object_store::ObjectStore>,
        ::object_store::path::Path,
    ),
}

#[cfg(feature = "aws")]
use ::object_store::{GetOptions, ObjectStore, ObjectStoreExt};

impl ObjectReader {
    /// Report the object's size and identity.
    pub(crate) async fn stat(&self) -> Result<ObjectStat, Error> {
        match &self.source {
            Source::Local(path) => {
                let metadata = std::fs::metadata(path)
                    .map_err(|e| Error(format!("cannot stat {}: {e}", path.display())))?;
                Ok(ObjectStat {
                    size: metadata.len(),
                    identity: None,
                })
            }
            #[cfg(feature = "aws")]
            Source::Remote(store, location) => {
                let metadata = store.head(location).await.map_err(remote_error)?;
                Ok(ObjectStat {
                    size: metadata.size,
                    identity: metadata.e_tag.or(metadata.version),
                })
            }
        }
    }

    /// Read a bounded byte range, optionally pinned to a known identity.
    pub(crate) async fn read_range(
        &self,
        range: Range<u64>,
        identity: Option<&str>,
    ) -> Result<Vec<u8>, Error> {
        let _ = identity;
        match &self.source {
            Source::Local(path) => read_local(path, range),
            #[cfg(feature = "aws")]
            Source::Remote(store, location) => {
                let options = GetOptions::default()
                    .with_range(Some(range))
                    .with_if_match(identity.map(str::to_owned));
                let result = store
                    .get_opts(location, options)
                    .await
                    .map_err(remote_error)?;
                result
                    .bytes()
                    .await
                    .map_err(remote_error)
                    .map(bytes::Bytes::into)
            }
        }
    }
}

/// Open the object named by `uri`.
///
/// `file` URIs always work; `s3`/`s3a` URIs require the `aws` feature. Backend
/// options are passed through as `(key, value)` pairs.
pub(crate) fn open(uri: &str, options: &[(String, String)]) -> Result<ObjectReader, Error> {
    let url = Url::parse(uri).map_err(|e| Error(format!("invalid object URI {uri}: {e}")))?;
    match url.scheme() {
        "file" => {
            let path = url
                .to_file_path()
                .map_err(|()| Error(format!("invalid local file URI: {uri}")))?;
            Ok(ObjectReader {
                source: Source::Local(path),
            })
        }
        "s3" | "s3a" => s3(&url, options),
        scheme => Err(Error(format!(
            "unsupported object URI scheme `{scheme}`; supported schemes are file and s3"
        ))),
    }
}

#[cfg(feature = "aws")]
fn s3(url: &Url, options: &[(String, String)]) -> Result<ObjectReader, Error> {
    use ::object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};

    // The AWS_* environment supplies the defaults (credentials, region,
    // AWS_SKIP_SIGNATURE=true for public buckets); explicit options override it.
    let mut builder = AmazonS3Builder::from_env().with_url(url.to_string());
    for (key, value) in options {
        let config_key: AmazonS3ConfigKey = key
            .to_ascii_lowercase()
            .parse()
            .map_err(|_| Error(format!("unknown object store option `{key}`")))?;
        builder = builder.with_config(config_key, value.clone());
    }
    let store = builder.build().map_err(remote_error)?;
    let (_, location) =
        ::object_store::ObjectStoreScheme::parse(url).map_err(|e| Error(e.to_string()))?;
    Ok(ObjectReader {
        source: Source::Remote(Box::new(store), location),
    })
}

#[cfg(not(feature = "aws"))]
fn s3(_url: &Url, _options: &[(String, String)]) -> Result<ObjectReader, Error> {
    Err(Error(
        "object URI scheme `s3` requires the `aws` feature".into(),
    ))
}

#[cfg(feature = "aws")]
fn remote_error(error: ::object_store::Error) -> Error {
    Error(error.to_string())
}

fn read_local(path: &std::path::Path, range: Range<u64>) -> Result<Vec<u8>, Error> {
    use std::io::{Read, Seek, SeekFrom};

    let mut file = std::fs::File::open(path)
        .map_err(|e| Error(format!("cannot open {}: {e}", path.display())))?;
    file.seek(SeekFrom::Start(range.start))
        .map_err(|e| Error(format!("cannot seek {}: {e}", path.display())))?;
    let length = usize::try_from(range.end.saturating_sub(range.start))
        .map_err(|_| Error(format!("range too large for {}", path.display())))?;
    let mut buffer = vec![0; length];
    file.read_exact(&mut buffer)
        .map_err(|e| Error(format!("cannot read {}: {e}", path.display())))?;
    Ok(buffer)
}
