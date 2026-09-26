//! The S3 backend. This is the only module that names the `object_store` crate.

use std::future::Future;
use std::ops::Range;
use std::pin::Pin;

use ::object_store::aws::AmazonS3;
use ::object_store::signer::Signer;
use ::object_store::{GetOptions, ObjectStore, ObjectStoreExt};
use url::Url;

use crate::third_party::object_store::api::{
    Error, ObjectReader, ObjectStat, PrefixListing, Remote,
};

struct S3 {
    store: Box<dyn ObjectStore>,
    /// A concrete S3 store so a HEAD can be signed and its
    /// `x-amz-storage-class` read. Absent when the backend is not S3.
    signer: Option<AmazonS3>,
    location: ::object_store::path::Path,
}

impl Remote for S3 {
    fn exists<'a>(&'a self) -> Pin<Box<dyn Future<Output = Result<bool, Error>> + Send + 'a>> {
        Box::pin(async move {
            match self.store.head(&self.location).await {
                Ok(_) => Ok(true),
                Err(::object_store::Error::NotFound { .. }) => Ok(false),
                Err(error) => Err(remote_error(error)),
            }
        })
    }

    fn stat<'a>(&'a self) -> Pin<Box<dyn Future<Output = Result<ObjectStat, Error>> + Send + 'a>> {
        Box::pin(async move {
            if let Some(signer) = &self.signer {
                if let Ok(stat) = head_object(signer, &self.location).await {
                    return Ok(stat);
                }
            }
            stat_get_opts(self.store.as_ref(), &self.location).await
        })
    }

    fn read_range<'a>(
        &'a self,
        range: Range<u64>,
        identity: Option<String>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, Error>> + Send + 'a>> {
        Box::pin(async move {
            let options = GetOptions::default()
                .with_range(Some(range))
                .with_if_match(identity);
            let result = self
                .store
                .get_opts(&self.location, options)
                .await
                .map_err(remote_error)?;
            result
                .bytes()
                .await
                .map_err(remote_error)
                .map(bytes::Bytes::into)
        })
    }
}

/// Build the S3 reader for `url`.
pub(crate) fn open_remote(url: &Url, options: &[(String, String)]) -> Result<ObjectReader, Error> {
    let (store, signer, location) = s3_store(url, options)?;
    Ok(ObjectReader::from_remote(Box::new(S3 {
        store,
        signer,
        location,
    })))
}

/// One pooled client for every signed HEAD, so connections are reused across
/// the files of a table.
fn head_client() -> &'static reqwest::Client {
    static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    CLIENT.get_or_init(reqwest::Client::new)
}

/// object_store 0.13 maps cache/content headers on HEAD but drops
/// `x-amz-storage-class`. Sign a HEAD and read the header ourselves.
async fn head_object(
    store: &AmazonS3,
    location: &::object_store::path::Path,
) -> Result<ObjectStat, Error> {
    let url = store
        .signed_url(
            reqwest::Method::HEAD,
            location,
            std::time::Duration::from_secs(60),
        )
        .await
        .map_err(remote_error)?;
    let response = head_client()
        .head(url)
        .send()
        .await
        .map_err(|error| Error(format!("cannot HEAD object: {error}")))?;
    if !response.status().is_success() {
        return Err(Error(format!(
            "cannot HEAD object: HTTP {}",
            response.status()
        )));
    }
    let headers: Vec<(String, String)> = response
        .headers()
        .iter()
        .filter_map(|(name, value)| {
            Some((name.as_str().to_string(), value.to_str().ok()?.to_string()))
        })
        .collect();
    let pairs: Vec<(&str, &str)> = headers
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .collect();
    head_stat(&pairs).ok_or_else(|| Error("HEAD response has no object size".into()))
}

async fn stat_get_opts(
    store: &dyn ObjectStore,
    location: &::object_store::path::Path,
) -> Result<ObjectStat, Error> {
    let result = store
        .get_opts(
            location,
            GetOptions {
                head: true,
                ..GetOptions::default()
            },
        )
        .await
        .map_err(remote_error)?;
    let metadata = result.meta;
    Ok(ObjectStat {
        size_bytes: metadata.size,
        identity: metadata.e_tag.or(metadata.version),
        storage_class: result
            .attributes
            .get(&::object_store::Attribute::StorageClass)
            .map(|value| value.as_ref().to_string()),
    })
}

/// Parse size, identity, and storage class from a HEAD response.
fn head_stat(headers: &[(&str, &str)]) -> Option<ObjectStat> {
    let mut size = None;
    let mut etag = None;
    let mut version = None;
    let mut storage_class = None;
    for (name, value) in headers {
        match name.to_ascii_lowercase().as_str() {
            "content-length" => size = value.parse().ok(),
            "etag" => etag = Some((*value).to_string()),
            "x-amz-version-id" => version = Some((*value).to_string()),
            "x-amz-storage-class" | "x-goog-storage-class" if !value.is_empty() => {
                storage_class = Some((*value).to_string());
            }
            _ => {}
        }
    }
    Some(ObjectStat {
        size_bytes: size?,
        identity: etag.or(version),
        storage_class,
    })
}

/// List one level of children under `url` with S3's delimiter. Listing does not
/// recurse, so a table walk reads one prefix per round trip.
pub(crate) async fn list_remote(
    url: &Url,
    options: &[(String, String)],
) -> Result<PrefixListing, Error> {
    let (store, _signer, location) = s3_store(url, options)?;
    let prefix = (!location.as_ref().is_empty()).then_some(&location);
    let result = store
        .list_with_delimiter(prefix)
        .await
        .map_err(remote_error)?;
    Ok(PrefixListing {
        prefixes: result
            .common_prefixes
            .iter()
            .map(|prefix| child_name(location.as_ref(), prefix.as_ref()))
            .collect(),
        objects: result
            .objects
            .iter()
            .map(|object| child_name(location.as_ref(), object.location.as_ref()))
            .filter(|name| !name.is_empty() && !name.contains('/'))
            .collect(),
    })
}

type S3Store = (
    Box<dyn ObjectStore>,
    Option<AmazonS3>,
    ::object_store::path::Path,
);

fn s3_store(url: &Url, options: &[(String, String)]) -> Result<S3Store, Error> {
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
    Ok((Box::new(store.clone()), Some(store), location))
}

fn child_name(parent: &str, child: &str) -> String {
    let parent = parent.trim_end_matches('/');
    let child = child.trim_end_matches('/');
    let relative = match parent.is_empty() {
        true => child,
        false => child.strip_prefix(parent).unwrap_or(child),
    };
    relative.trim_start_matches('/').to_string()
}

fn remote_error(error: ::object_store::Error) -> Error {
    Error(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use ::object_store::memory::InMemory;
    use ::object_store::path::Path;
    use ::object_store::throttle::{ThrottleConfig, ThrottledStore};
    use ::object_store::ObjectStoreExt;
    use tokio::task::JoinSet;

    use super::S3;
    use crate::third_party::object_store::api::ObjectReader;

    /// Same-region S3 per-request latency: p50 ~25 ms (topicpartition.io 2025
    /// measured 26 ms; AWS CloudWatch percentiles ~25 ms), p99 ~100 ms.
    const S3_LATENCY: Duration = Duration::from_millis(25);

    fn config() -> ThrottleConfig {
        ThrottleConfig {
            wait_get_per_call: S3_LATENCY,
            wait_list_per_call: S3_LATENCY,
            wait_list_with_delimiter_per_call: S3_LATENCY,
            ..ThrottleConfig::default()
        }
    }

    async fn reader() -> ObjectReader {
        let path = Path::from("data");
        let store = ThrottledStore::new(InMemory::new(), config());
        store
            .put(&path, b"parquet footer bytes".as_slice().into())
            .await
            .unwrap();
        ObjectReader::from_remote(Box::new(S3 {
            store: Box::new(store),
            signer: None,
            location: path,
        }))
    }

    #[tokio::test]
    async fn remote_reads_pay_the_s3_latency() {
        let reader = reader().await;

        let start = Instant::now();
        let stat = reader.stat().await.unwrap();
        let stat_elapsed = start.elapsed();
        assert_eq!(stat.size_bytes, 20);
        assert!(stat_elapsed >= S3_LATENCY, "stat took {stat_elapsed:?}");

        let start = Instant::now();
        let bytes = reader.read_range(0..7, None).await.unwrap();
        let read_elapsed = start.elapsed();
        assert_eq!(bytes, b"parquet");
        assert!(read_elapsed >= S3_LATENCY, "read took {read_elapsed:?}");
    }

    #[tokio::test]
    async fn concurrent_remote_reads_share_one_latency() {
        let reader = Arc::new(reader().await);
        let mut set = JoinSet::new();
        let start = Instant::now();
        for _ in 0..4 {
            let reader = Arc::clone(&reader);
            set.spawn(async move { reader.read_range(0..7, None).await });
        }
        while let Some(done) = set.join_next().await {
            assert_eq!(done.unwrap().unwrap(), b"parquet");
        }
        let elapsed = start.elapsed();
        assert!(elapsed < 4 * S3_LATENCY, "4 reads serialized: {elapsed:?}");
    }

    #[test]
    fn head_reads_storage_class() {
        let stat = super::head_stat(&[
            ("Content-Length", "42"),
            ("ETag", "\"abc\""),
            ("x-amz-storage-class", "STANDARD_IA"),
        ])
        .unwrap();
        assert_eq!(stat.size_bytes, 42);
        assert_eq!(stat.identity.as_deref(), Some("\"abc\""));
        assert_eq!(stat.storage_class.as_deref(), Some("STANDARD_IA"));
    }

    #[test]
    fn head_omits_standard_when_s3_sends_no_class_header() {
        let stat = super::head_stat(&[("content-length", "8")]).unwrap();
        assert_eq!(stat.size_bytes, 8);
        assert!(stat.storage_class.is_none());
    }
}
