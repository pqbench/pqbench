//! The S3 backend. This is the only module that names the `object_store` crate.

use std::future::Future;
use std::ops::Range;
use std::pin::Pin;

use ::object_store::{GetOptions, ObjectStore, ObjectStoreExt};
use url::Url;

use crate::third_party::object_store::api::{
    Error, ObjectReader, ObjectStat, PrefixListing, Remote,
};

struct S3 {
    store: Box<dyn ObjectStore>,
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
            let result = self
                .store
                .get_opts(
                    &self.location,
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
    let (store, location) = s3_store(url, options)?;
    Ok(ObjectReader::from_remote(Box::new(S3 { store, location })))
}

/// List one level of children under `url` with S3's delimiter. Listing does not
/// recurse, so a table walk reads one prefix per round trip.
pub(crate) async fn list_remote(
    url: &Url,
    options: &[(String, String)],
) -> Result<PrefixListing, Error> {
    let (store, location) = s3_store(url, options)?;
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

fn s3_store(
    url: &Url,
    options: &[(String, String)],
) -> Result<(Box<dyn ObjectStore>, ::object_store::path::Path), Error> {
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
    Ok((Box::new(store), location))
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
}
