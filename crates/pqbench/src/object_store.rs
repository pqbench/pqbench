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
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

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
    /// Last modification time as RFC3339 UTC, when the store reports one.
    pub last_modified_time: Option<String>,
    /// Creation time as RFC3339 UTC, when the store reports one.
    pub creation_time: Option<String>,
    /// Storage class or tier (`STANDARD_IA`, `GLACIER`, …), when HEAD
    /// reports `x-amz-storage-class`. S3 omits the header for `STANDARD`.
    pub storage_class: Option<String>,
}

/// A random-access handle to a single object (local or remote).
pub(crate) struct ObjectReader {
    source: Source,
}

enum Source {
    Local(PathBuf),
    #[cfg(feature = "aws")]
    Remote(::object_store::aws::AmazonS3, ::object_store::path::Path),
}

#[cfg(feature = "aws")]
use ::object_store::signer::Signer;
#[cfg(feature = "aws")]
use ::object_store::{GetOptions, ObjectStore, ObjectStoreExt};

impl ObjectReader {
    /// Whether the object exists. A missing object is `Ok(false)`.
    pub(crate) async fn exists(&self) -> Result<bool, Error> {
        match &self.source {
            Source::Local(path) => Ok(path.exists()),
            #[cfg(feature = "aws")]
            Source::Remote(store, location) => match store.head(location).await {
                Ok(_) => Ok(true),
                Err(::object_store::Error::NotFound { .. }) => Ok(false),
                Err(error) => Err(remote_error(error)),
            },
        }
    }

    /// Report the object's size and identity.
    pub(crate) async fn stat(&self) -> Result<ObjectStat, Error> {
        match &self.source {
            Source::Local(path) => stat_local(path),
            #[cfg(feature = "aws")]
            Source::Remote(store, location) => match head_object(store, location).await {
                Ok(stat) => Ok(stat),
                Err(_) => stat_get_opts(store, location).await,
            },
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

/// One level of names under a prefix. Listing does not recurse.
pub(crate) struct PrefixListing {
    /// Child prefix names. A table walk stops when one of these is a marker.
    pub prefixes: Vec<String>,
    /// Object names at this level (one path component).
    pub objects: Vec<String>,
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

/// List one level of children under `uri`. `s3`/`s3a` require the `aws` feature.
pub(crate) async fn list_prefix(
    uri: &str,
    options: &[(String, String)],
) -> Result<PrefixListing, Error> {
    let url = Url::parse(uri).map_err(|e| Error(format!("invalid object URI {uri}: {e}")))?;
    match url.scheme() {
        "file" => list_file(&url),
        "s3" | "s3a" => list_s3(&url, options).await,
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
        if entry.path().is_dir() {
            prefixes.push(name);
        } else {
            objects.push(name);
        }
    }
    Ok(PrefixListing { prefixes, objects })
}

#[cfg(feature = "aws")]
fn s3(url: &Url, options: &[(String, String)]) -> Result<ObjectReader, Error> {
    let (store, location) = s3_store(url, options)?;
    Ok(ObjectReader {
        source: Source::Remote(store, location),
    })
}

/// object_store 0.13 maps cache/content headers on HEAD but drops
/// `x-amz-storage-class`. Sign a HEAD and read the header ourselves.
#[cfg(feature = "aws")]
async fn head_object(
    store: &::object_store::aws::AmazonS3,
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
    let response = reqwest::Client::new()
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

#[cfg(feature = "aws")]
async fn stat_get_opts(
    store: &::object_store::aws::AmazonS3,
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
        size: metadata.size,
        identity: metadata.e_tag.or(metadata.version),
        last_modified_time: unix_timestamp_rfc3339(metadata.last_modified.timestamp()),
        creation_time: None,
        storage_class: result
            .attributes
            .get(&::object_store::Attribute::StorageClass)
            .map(|value| value.as_ref().to_string()),
    })
}

/// Parse size, identity, mtime, and storage class from a HEAD response.
#[cfg(any(test, feature = "aws"))]
fn head_stat(headers: &[(&str, &str)]) -> Option<ObjectStat> {
    let mut size = None;
    let mut etag = None;
    let mut version = None;
    let mut last_modified_time = None;
    let mut storage_class = None;
    for (name, value) in headers {
        match name.to_ascii_lowercase().as_str() {
            "content-length" => size = value.parse().ok(),
            "etag" => etag = Some((*value).to_string()),
            "x-amz-version-id" => version = Some((*value).to_string()),
            "last-modified" => last_modified_time = parse_http_date(value),
            "x-amz-storage-class" | "x-goog-storage-class" if !value.is_empty() => {
                storage_class = Some((*value).to_string());
            }
            _ => {}
        }
    }
    Some(ObjectStat {
        size: size?,
        identity: etag.or(version),
        last_modified_time,
        creation_time: None,
        storage_class,
    })
}

/// IMF-fixdate (`Wed, 23 Sep 2026 20:53:00 GMT`) to RFC3339 UTC.
#[cfg(any(test, feature = "aws"))]
fn parse_http_date(value: &str) -> Option<String> {
    let rest = value.split_once(", ")?.1;
    let mut parts = rest.split_whitespace();
    let day: u32 = parts.next()?.parse().ok()?;
    let month = http_month(parts.next()?)?;
    let year: i32 = parts.next()?.parse().ok()?;
    let mut time = parts.next()?.split(':');
    let hour: u32 = time.next()?.parse().ok()?;
    let minute: u32 = time.next()?.parse().ok()?;
    let second: u32 = time.next()?.parse().ok()?;
    if parts.next() != Some("GMT") {
        return None;
    }
    if !(1..=12).contains(&month) || day == 0 || hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let days = days_from_civil(year, month, day);
    let secs = days
        .checked_mul(86_400)?
        .checked_add(i64::from(hour * 3_600 + minute * 60 + second))?;
    unix_timestamp_rfc3339(secs)
}

#[cfg(any(test, feature = "aws"))]
fn http_month(name: &str) -> Option<u32> {
    Some(match name {
        "Jan" => 1,
        "Feb" => 2,
        "Mar" => 3,
        "Apr" => 4,
        "May" => 5,
        "Jun" => 6,
        "Jul" => 7,
        "Aug" => 8,
        "Sep" => 9,
        "Oct" => 10,
        "Nov" => 11,
        "Dec" => 12,
        _ => return None,
    })
}

#[cfg(feature = "aws")]
async fn list_s3(url: &Url, options: &[(String, String)]) -> Result<PrefixListing, Error> {
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

#[cfg(feature = "aws")]
fn s3_store(
    url: &Url,
    options: &[(String, String)],
) -> Result<(::object_store::aws::AmazonS3, ::object_store::path::Path), Error> {
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
    Ok((store, location))
}

#[cfg(feature = "aws")]
fn child_name(parent: &str, child: &str) -> String {
    let parent = parent.trim_end_matches('/');
    let child = child.trim_end_matches('/');
    let relative = match parent.is_empty() {
        true => child,
        false => child.strip_prefix(parent).unwrap_or(child),
    };
    relative.trim_start_matches('/').to_string()
}

#[cfg(not(feature = "aws"))]
fn s3(_url: &Url, _options: &[(String, String)]) -> Result<ObjectReader, Error> {
    Err(Error(
        "object URI scheme `s3` requires the `aws` feature".into(),
    ))
}

#[cfg(not(feature = "aws"))]
async fn list_s3(_url: &Url, _options: &[(String, String)]) -> Result<PrefixListing, Error> {
    Err(Error(
        "object URI scheme `s3` requires the `aws` feature".into(),
    ))
}

#[cfg(feature = "aws")]
fn remote_error(error: ::object_store::Error) -> Error {
    Error(error.to_string())
}

/// Stat a local path. Used by footer reads that already have a filesystem path.
pub(crate) fn stat_local(path: &Path) -> Result<ObjectStat, Error> {
    let metadata = std::fs::metadata(path)
        .map_err(|e| Error(format!("cannot stat {}: {e}", path.display())))?;
    Ok(ObjectStat {
        size: metadata.len(),
        identity: None,
        last_modified_time: metadata.modified().ok().and_then(system_time_rfc3339),
        creation_time: metadata.created().ok().and_then(system_time_rfc3339),
        storage_class: None,
    })
}

fn system_time_rfc3339(time: SystemTime) -> Option<String> {
    unix_timestamp_rfc3339(i64::try_from(time.duration_since(UNIX_EPOCH).ok()?.as_secs()).ok()?)
}

fn unix_timestamp_rfc3339(secs: i64) -> Option<String> {
    let secs = u64::try_from(secs).ok()?;
    Some(unix_secs_rfc3339(secs))
}

/// Format epoch milliseconds as RFC3339 UTC seconds.
#[cfg(any(feature = "delta", feature = "iceberg"))]
pub(crate) fn unix_millis_rfc3339(millis: i64) -> Option<String> {
    unix_timestamp_rfc3339(millis.checked_div(1000)?)
}

/// Parse `YYYY-MM-DD` or `YYYY-MM-DDTHH:MM:SSZ` to epoch milliseconds.
pub(crate) fn parse_rfc3339_millis(value: &str) -> Result<i64, Error> {
    let (date, time) = match value.split_once('T') {
        Some((date, time)) => (date, time.trim_end_matches('Z')),
        None => (value, "00:00:00"),
    };
    let mut date_parts = date.split('-');
    let year: i32 = parse_time_part(date_parts.next(), "year", value)?;
    let month: u32 = parse_time_part(date_parts.next(), "month", value)?;
    let day: u32 = parse_time_part(date_parts.next(), "day", value)?;
    if date_parts.next().is_some() {
        return Err(Error(format!("invalid time `{value}`")));
    }
    let mut time_parts = time.split(':');
    let hour: u32 = parse_time_part(time_parts.next(), "hour", value)?;
    let minute: u32 = parse_time_part(time_parts.next(), "minute", value)?;
    let second: u32 = parse_time_part(time_parts.next(), "second", value)?;
    if time_parts.next().is_some()
        || !(1..=12).contains(&month)
        || day == 0
        || hour > 23
        || minute > 59
        || second > 59
    {
        return Err(Error(format!("invalid time `{value}`")));
    }
    let days = days_from_civil(year, month, day);
    let secs = days
        .checked_mul(86_400)
        .and_then(|days| days.checked_add(i64::from(hour * 3_600 + minute * 60 + second)))
        .ok_or_else(|| Error(format!("invalid time `{value}`")))?;
    secs.checked_mul(1000)
        .ok_or_else(|| Error(format!("invalid time `{value}`")))
}

fn parse_time_part<T: std::str::FromStr>(
    part: Option<&str>,
    name: &str,
    value: &str,
) -> Result<T, Error> {
    part.ok_or_else(|| Error(format!("invalid time `{value}`: missing {name}")))?
        .parse()
        .map_err(|_| Error(format!("invalid time `{value}`")))
}

fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let year = if month <= 2 {
        i64::from(year) - 1
    } else {
        i64::from(year)
    };
    let month = i64::from(month);
    let day = i64::from(day);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let doy = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// UTC civil time as `YYYY-MM-DDTHH:MM:SSZ` (Howard Hinnant, days from Unix epoch).
fn unix_secs_rfc3339(secs: u64) -> String {
    let days = secs / 86_400;
    let tod = secs % 86_400;
    let hour = tod / 3_600;
    let min = (tod % 3_600) / 60;
    let sec = tod % 60;
    let (year, month, day) = civil_from_days(i64::try_from(days).unwrap_or(i64::MAX));
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}Z")
}

fn civil_from_days(days: i64) -> (i32, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = u64::try_from(z - era * 146_097).unwrap_or(0);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let year = i32::try_from(i64::try_from(yoe).unwrap_or(0) + era * 400).unwrap_or(i32::MAX);
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = if month <= 2 { year + 1 } else { year };
    (year, month, day)
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

#[cfg(test)]
mod tests {
    use super::unix_secs_rfc3339;

    #[test]
    fn unix_epoch_is_rfc3339_utc() {
        assert_eq!(unix_secs_rfc3339(0), "1970-01-01T00:00:00Z");
        assert_eq!(unix_secs_rfc3339(1_000_000_000), "2001-09-09T01:46:40Z");
        assert_eq!(
            super::parse_rfc3339_millis("2001-09-09T01:46:40Z").unwrap(),
            1_000_000_000_000
        );
        assert_eq!(super::parse_rfc3339_millis("1970-01-01").unwrap(), 0);
    }

    #[test]
    fn head_reads_storage_class_and_http_date() {
        let stat = super::head_stat(&[
            ("Content-Length", "42"),
            ("ETag", "\"abc\""),
            ("Last-Modified", "Sun, 09 Sep 2001 01:46:40 GMT"),
            ("x-amz-storage-class", "STANDARD_IA"),
        ])
        .unwrap();
        assert_eq!(stat.size, 42);
        assert_eq!(stat.identity.as_deref(), Some("\"abc\""));
        assert_eq!(
            stat.last_modified_time.as_deref(),
            Some("2001-09-09T01:46:40Z")
        );
        assert_eq!(stat.storage_class.as_deref(), Some("STANDARD_IA"));
    }

    #[test]
    fn head_omits_standard_when_s3_sends_no_class_header() {
        let stat = super::head_stat(&[("content-length", "8")]).unwrap();
        assert_eq!(stat.size, 8);
        assert!(stat.storage_class.is_none());
    }
}
