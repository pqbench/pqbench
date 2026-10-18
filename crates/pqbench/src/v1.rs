//! The pqbench wire types, and the one home for resource names.
//!
//! One definition per resource and value, so a command and the CLI share a
//! type instead of each declaring its own. The CLI forwards these as-is (it
//! renders the human table for them); the library's commands return them.
//!
//! These are ordinary Rust types first: a value that can be absent is an
//! `Option`, a quantity is unsigned, an enum has no "unspecified" sentinel
//! (absence is `None`), and a collection is a `Vec`/`BTreeMap`. The
//! `proposals/pqbench.proto` is the design sketch and the wire contract; where
//! a proto idiom (a zero-value variant, an `int64`, a prefixed enum value) is
//! not the Rust idiom, Rust wins and the wire keeps the proto's spelling via
//! `serde`.
//!
//! [`name`] is the one place a name format lives, so changing it (say, to a
//! URN) is a one-module change.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Resource names (AIP-122 paths): build and parse them in one place.
///
/// A name starts with its collection (`catalogs/{catalog}`, `metastores/{metastore}`,
/// `blobs/{service}/{locator}`, …) and every id segment is percent-encoded to one URI path
/// segment, so a name with a `/` in an id (a locator, a dotted column path)
/// still decodes back to what it names.
pub mod name {
    use crate::dialect::encode;

    /// `metastores/{metastore}`.
    #[must_use]
    pub fn metastore(metastore: &str) -> String {
        format!("metastores/{}", encode(metastore))
    }

    /// `catalogs/{catalog}`.
    #[must_use]
    pub fn catalog(catalog: &str) -> String {
        format!("catalogs/{}", encode(catalog))
    }

    /// `catalogs/{catalog}/schemas/{schema}`.
    #[must_use]
    pub fn schema(catalog: &str, schema: &str) -> String {
        format!("{}/schemas/{}", self::catalog(catalog), encode(schema))
    }

    /// `…/schemas/{schema}/tables/{table}`.
    #[must_use]
    pub fn table(catalog: &str, schema: &str, table: &str) -> String {
        format!("{}/tables/{}", self::schema(catalog, schema), encode(table))
    }

    /// `…/tables/{table}/partitions/{partition}`.
    #[must_use]
    pub fn partition(catalog: &str, schema: &str, table: &str, partition: &str) -> String {
        self::partition_name(&self::table(catalog, schema, table), partition)
    }

    /// `{table}/partitions/{partition}` — a partition under a known table name.
    #[must_use]
    pub fn partition_name(table: &str, partition: &str) -> String {
        format!("{table}/partitions/{}", encode(partition))
    }

    /// `…/tables/{table}/files/{file}`.
    #[must_use]
    pub fn table_file(catalog: &str, schema: &str, table: &str, file: &str) -> String {
        self::table_file_name(&self::table(catalog, schema, table), file)
    }

    /// `{table}/files/{file}` — a file under a known table name.
    #[must_use]
    pub fn table_file_name(table: &str, file: &str) -> String {
        format!("{table}/files/{}", encode(file))
    }

    /// `{table}/commits/{commit}` — a commit under a known table name.
    #[must_use]
    pub fn commit_name(table: &str, commit: &str) -> String {
        format!("{table}/commits/{}", encode(commit))
    }

    /// `…/tables/{table}/commits/{commit}`.
    #[must_use]
    pub fn commit(catalog: &str, schema: &str, table: &str, commit: &str) -> String {
        format!(
            "{}/commits/{}",
            self::table(catalog, schema, table),
            encode(commit)
        )
    }

    /// `blobs/{service}/{locator}`.
    #[must_use]
    pub fn blob(service: &str, locator: &str) -> String {
        format!("blobs/{}/{}", encode(service), encode(locator))
    }

    /// `…/columns/{column}`.
    #[must_use]
    pub fn column(service: &str, locator: &str, column: &str) -> String {
        format!(
            "{}/columns/{}",
            self::blob(service, locator),
            encode(column)
        )
    }

    /// The metastore id in a `metastores/{metastore}` name.
    #[must_use]
    pub fn parse_metastore(value: &str) -> Option<String> {
        match split(value).as_slice() {
            ["metastores", metastore] => Some(decode(metastore)),
            _ => None,
        }
    }

    /// The catalog id in a `catalogs/{catalog}` name.
    #[must_use]
    pub fn parse_catalog(value: &str) -> Option<String> {
        match split(value).as_slice() {
            ["catalogs", catalog] => Some(decode(catalog)),
            _ => None,
        }
    }

    /// The catalog and schema in a `…/schemas/{schema}` name.
    #[must_use]
    pub fn parse_schema(value: &str) -> Option<(String, String)> {
        match split(value).as_slice() {
            ["catalogs", catalog, "schemas", schema] => Some((decode(catalog), decode(schema))),
            _ => None,
        }
    }

    /// The catalog, schema, and table in a `…/tables/{table}` name.
    #[must_use]
    pub fn parse_table(value: &str) -> Option<(String, String, String)> {
        match split(value).as_slice() {
            ["catalogs", catalog, "schemas", schema, "tables", table] => {
                Some((decode(catalog), decode(schema), decode(table)))
            }
            _ => None,
        }
    }

    /// The catalog, schema, table, and partition in a `…/partitions/{…}` name.
    #[must_use]
    pub fn parse_partition(value: &str) -> Option<(String, String, String, String)> {
        match split(value).as_slice() {
            ["catalogs", catalog, "schemas", schema, "tables", table, "partitions", partition] => {
                Some((
                    decode(catalog),
                    decode(schema),
                    decode(table),
                    decode(partition),
                ))
            }
            _ => None,
        }
    }

    /// The `local` catalog and schema a path-read table lives in: a storage
    /// root no catalog named is still a resource, and these give it one.
    const LOCAL: &str = "local";

    /// The name a table read by path takes: the `local` catalog and schema
    /// around the storage root's last path segment. The root itself stays on
    /// the [`Table`] as its `uri`, so nothing is lost.
    ///
    /// [`Table`]: super::Table
    #[must_use]
    pub fn local_table(uri: &str) -> String {
        let root = uri.trim_end_matches('/');
        let id = root
            .rsplit('/')
            .find(|segment| !segment.is_empty())
            .unwrap_or(root);
        table(LOCAL, LOCAL, id)
    }

    /// The `blobs/{service}/{locator}` name a storage URI or local path names.
    ///
    /// The service is the URI scheme (`s3`, `file`, …); a local path (no
    /// scheme, or `file://`) is `file`. The locator is the rest, so the name
    /// decodes back to what the URI reads.
    #[must_use]
    pub fn blob_name(uri: &str) -> String {
        let (service, locator) = storage_parts(uri);
        blob(&service, &locator)
    }

    /// The storage URI a `blobs/{service}/{locator}` name reads. `None` when
    /// the name is not a blob name.
    #[must_use]
    pub fn blob_uri(value: &str) -> Option<String> {
        let (service, locator) = parse_blob(value)?;
        Some(storage_uri(&service, &locator))
    }

    /// The service and locator in a `blobs/{service}/{locator}` name.
    #[must_use]
    pub fn parse_blob(value: &str) -> Option<(String, String)> {
        match split(value).as_slice() {
            ["blobs", service, locator] => Some((decode(service), decode(locator))),
            _ => None,
        }
    }

    /// The table name and file path in a `…/tables/{table}/files/{file}` name.
    #[must_use]
    pub fn parse_table_file(value: &str) -> Option<(String, String)> {
        let (table, file) = value.rsplit_once("/files/")?;
        parse_table(table)?;
        Some((table.to_string(), decode(file)))
    }

    /// Split a storage URI or local path into a blob's service and locator.
    fn storage_parts(uri: &str) -> (String, String) {
        if let Some(locator) = uri.strip_prefix("file://") {
            return ("file".to_string(), locator.to_string());
        }
        match uri.split_once("://") {
            Some((scheme, locator)) if !scheme.is_empty() => {
                (scheme.to_string(), locator.to_string())
            }
            _ => ("file".to_string(), uri.to_string()),
        }
    }

    /// The URI a blob's service and locator read. A `file` blob is its bare
    /// path, which the local readers accept.
    fn storage_uri(service: &str, locator: &str) -> String {
        if service == "file" {
            locator.to_string()
        } else {
            format!("{service}://{locator}")
        }
    }

    fn split(value: &str) -> Vec<&str> {
        value.split('/').collect()
    }

    /// Percent-decode one path segment back to its id.
    fn decode(value: &str) -> String {
        let bytes = value.as_bytes();
        let mut out = Vec::with_capacity(bytes.len());
        let mut index = 0;
        while index < bytes.len() {
            if bytes[index] == b'%' && index + 2 < bytes.len() {
                if let (Some(high), Some(low)) = (hex(bytes[index + 1]), hex(bytes[index + 2])) {
                    out.push(high * 16 + low);
                    index += 3;
                    continue;
                }
            }
            out.push(bytes[index]);
            index += 1;
        }
        String::from_utf8_lossy(&out).into_owned()
    }

    fn hex(byte: u8) -> Option<u8> {
        (byte as char).to_digit(16).map(|value| value as u8)
    }
}

/// A catalog's kind. Absence is `None` on the field, not a variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum CatalogType {
    #[serde(rename = "CATALOG_TYPE_MANAGED")]
    Managed,
    #[serde(rename = "CATALOG_TYPE_FOREIGN")]
    Foreign,
    #[serde(rename = "CATALOG_TYPE_SYSTEM")]
    System,
}

impl CatalogType {
    /// The proto value name, as the wire spells it.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Managed => "CATALOG_TYPE_MANAGED",
            Self::Foreign => "CATALOG_TYPE_FOREIGN",
            Self::System => "CATALOG_TYPE_SYSTEM",
        }
    }
}

/// An on-disk table format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum TableFormat {
    #[serde(rename = "TABLE_FORMAT_DELTA")]
    Delta,
    #[serde(rename = "TABLE_FORMAT_ICEBERG")]
    Iceberg,
}

impl TableFormat {
    /// The proto value name, as the wire spells it.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Delta => "TABLE_FORMAT_DELTA",
            Self::Iceberg => "TABLE_FORMAT_ICEBERG",
        }
    }
}

/// Why a table is not readable outside Databricks compute.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum EligibilityReason {
    #[serde(rename = "ELIGIBILITY_REASON_SYSTEM_TABLE")]
    SystemTable,
    #[serde(rename = "ELIGIBILITY_REASON_NOT_A_TABLE")]
    NotATable,
    #[serde(rename = "ELIGIBILITY_REASON_NO_EXTERNAL_READ")]
    NoExternalRead,
}

impl EligibilityReason {
    /// The proto value name, as the wire spells it.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SystemTable => "ELIGIBILITY_REASON_SYSTEM_TABLE",
            Self::NotATable => "ELIGIBILITY_REASON_NOT_A_TABLE",
            Self::NoExternalRead => "ELIGIBILITY_REASON_NO_EXTERNAL_READ",
        }
    }
}

/// The metastore: the entity above catalogs.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Metastore {
    /// `metastores/{metastore}`.
    pub name: String,
    /// The system UUID, when the endpoint reports one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
    /// The user-set metastore name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    /// The cloud the metastore lives in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cloud_provider: Option<String>,
    /// The cloud region the metastore lives in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
}

/// One catalog.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Catalog {
    /// `catalogs/{catalog}`.
    pub name: String,
    /// The system UUID, when the endpoint reports one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
    /// A display name distinct from the id, when the endpoint reports one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    /// The catalog kind, when the endpoint reports one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog_type: Option<CatalogType>,
    /// The catalog comment, when it has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    /// The catalog owner, when the endpoint reports one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
}

/// One schema.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Schema {
    /// `catalogs/{catalog}/schemas/{schema}`.
    pub name: String,
    /// The system UUID, when the endpoint reports one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
    /// The schema comment, when it has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    /// The schema's storage URI, when it has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uri: Option<String>,
    /// The schema properties the endpoint reports.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub properties: BTreeMap<String, String>,
}

/// One table's record.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Table {
    /// `catalogs/{catalog}/schemas/{schema}/tables/{table}`.
    pub name: String,
    /// The system UUID, when the endpoint reports one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
    /// The storage root URI.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uri: Option<String>,
    /// The detected table format, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<TableFormat>,
    /// Snapshot version the record describes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_version: Option<u64>,
    /// Partition columns; they need not occupy Parquet columns.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub partition_columns: Vec<String>,
    /// The schema, in order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub declared_columns: Vec<DeclaredColumn>,
    /// The table's properties (Delta configuration, Iceberg properties).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub properties: BTreeMap<String, String>,
    /// Active data files in the snapshot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_count: Option<u64>,
    /// The snapshot's partitions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition_count: Option<u64>,
    /// The vended storage credentials, when the caller holds them. Held on the
    /// table so a downstream stage reads the table's own lease.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credentials: Option<Credentials>,
}

/// One column of a table's schema. Not a resource: a bounded value on its
/// [`Table`], while a file's copy of it is the [`Column`] resource.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DeclaredColumn {
    /// The column's name within the schema.
    pub column: String,
    /// The data type as the format reports it (e.g. `long`, `string`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_type: Option<String>,
    /// Whether the column accepts nulls.
    #[serde(default)]
    pub nullable: bool,
    /// The column's position in the schema.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub position: Option<u64>,
}

/// A natural partition: a half-open commit-time window.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Partition {
    /// `…/tables/{table}/partitions/{partition}`.
    pub name: String,
    /// Inclusive start of the window, milliseconds since the Unix epoch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_time: Option<u64>,
    /// Exclusive end of the window, milliseconds since the Unix epoch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_time: Option<u64>,
    /// Commits in the window, in version order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub commit_versions: Vec<u64>,
    /// The commits' `commitInfo` actions, index-aligned with
    /// [`commit_versions`](Self::commit_versions): the operation, its
    /// parameters and metrics, and the writer's identity, as the log stores
    /// them. Empty when no commit in the window has one — Iceberg carries none,
    /// and a Delta writer may leave it out.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub commit_info: Vec<serde_json::Value>,
    /// The table's storage root URI. Denormalized onto the partition so the
    /// files stage reads the log without a catalog call.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uri: Option<String>,
    /// The table's vended credentials, when the caller holds them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credentials: Option<Credentials>,
}

/// A storage blob: the bytes a data file lives in — a local file or an S3
/// object, named by its service.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Blob {
    /// `blobs/{service}/{locator}`.
    pub name: String,
    /// The object's size in bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    /// The storage class or tier (`STANDARD`, `STANDARD_IA`, …), when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_class: Option<String>,
    /// Row groups in the file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_group_count: Option<u64>,
    /// Leaf columns in the file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column_count: Option<u64>,
}

/// A table's reference to one [`Blob`]: a row of the table's log (the Delta
/// `add`). The AIP-124 association between them.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TableFile {
    /// `…/tables/{table}/files/{file}`.
    pub name: String,
    /// The blob holding the bytes, a [`Blob`] name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blob: Option<String>,
    /// The table's storage root, denormalized onto the file so a reader reads
    /// [`Self::blob`] without the table record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_root: Option<String>,
    /// The path as the log records it, relative to [`Self::storage_root`].
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub path: String,
    /// The size the log claims, in bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    /// Partition values from the log; a value is `None` for a null partition.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub partition_values: BTreeMap<String, Option<String>>,
    /// The log's statistics, when it carries them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stats: Option<FileStats>,
    /// The vended storage credentials to read [`Self::blob`] with, when the
    /// caller holds them. A sibling to the resource on the wire, like the
    /// lease on a [`Table`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credentials: Option<Credentials>,
}

impl TableFile {
    /// The storage URI the bytes are at, from [`Self::blob`]. `None` when the
    /// record carries no blob.
    #[must_use]
    pub fn uri(&self) -> Option<String> {
        self.blob.as_deref().and_then(name::blob_uri)
    }
}

/// One commit of a table's log.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Commit {
    /// `…/tables/{table}/commits/{commit}`.
    pub name: String,
    /// The log version.
    pub version: u64,
    /// Commit time, milliseconds since the Unix epoch, when the log records
    /// one. Delta reads `commitInfo.timestamp`; Iceberg the snapshot time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_time: Option<i64>,
    /// The commit's `commitInfo` action as the log stores it, when it has one:
    /// the operation, its parameters and metrics, and the writer's identity.
    /// Delta reads it from the first line of the commit file; Iceberg has none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_info: Option<serde_json::Value>,
    /// Actions from the commit file, in file order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<LogAction>,
}

/// A file's copy of a declared column: the physical unit the analyses read.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Column {
    /// `…/blobs/{service}/{locator}/columns/{column}`.
    pub name: String,
    /// The declared column this file column is a copy of.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub declared_column: Option<String>,
    /// The column's position in the file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub position: Option<u64>,
    /// The physical type of the leaf column.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub physical_data_type: Option<String>,
    /// Row groups the column spans.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_group_count: Option<u64>,
}

/// A column's byte mass (the footer's `ColumnMetaData`), aggregated by column:
/// one per column, over its column chunks.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Bytemass {
    /// `…/columns/{column}/bytemass`.
    pub name: String,
    /// On-disk (compressed) bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compressed_bytes: Option<u64>,
    /// Encoded bytes before compression.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uncompressed_bytes: Option<u64>,
    /// The compression codec.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub codec: Option<String>,
    /// Encodings listed on the column chunks.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub encodings: Vec<String>,
    /// A dictionary page is present.
    #[serde(default)]
    pub dictionary: bool,
    /// Values in the column (including nulls).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_count: Option<u64>,
    /// Footer `null_count`, when statistics exist.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub null_count: Option<u64>,
    /// Footer `distinct_count`, when statistics exist.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub distinct_count: Option<u64>,
    /// Footer first value, when statistics exist.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_value: Option<String>,
    /// Footer last value, when statistics exist.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_value: Option<String>,
    /// Compressed bytes divided by [`Self::value_count`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes_per_value: Option<f64>,
    /// Uncompressed divided by compressed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compression_ratio: Option<f64>,
    /// [`Self::null_count`] divided by [`Self::value_count`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub null_fraction: Option<f64>,
}

/// A codec sweep of a column.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct CompressionReport {
    /// `…/columns/{column}/compression/{codec}`.
    pub name: String,
    /// The codec swept.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub codec: Option<String>,
    /// Compressed bytes for this codec.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compressed_bytes: Option<u64>,
    /// Uncompressed divided by compressed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ratio: Option<f64>,
    /// Compress throughput, in megabytes per second.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compress_megabytes_per_second: Option<f64>,
    /// Decompress throughput, in megabytes per second.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decompress_megabytes_per_second: Option<f64>,
}

/// A decoded-sample profile of a column.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ColumnProfile {
    /// `…/columns/{column}/profile`.
    pub name: String,
    /// Nulls among the sampled rows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub null_count: Option<u64>,
    /// Distinct values among the sampled rows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub distinct_count: Option<u64>,
    /// Shannon entropy, in bits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entropy_bits: Option<f64>,
    /// The most frequent values.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub top_values: Vec<String>,
}

/// Statistics copied from a log's `add` action.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct FileStats {
    /// `numRecords` from the log.
    pub record_count: u64,
    /// On-disk file size divided by [`Self::record_count`]. Absent at zero rows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes_per_row: Option<f64>,
    /// Per-column first value, JSON as stored (nested structs stay objects).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub first_values: BTreeMap<String, serde_json::Value>,
    /// Per-column last value, JSON as stored.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub last_values: BTreeMap<String, serde_json::Value>,
    /// Per-column `nullCount`. Nested struct columns are flattened to dotted
    /// keys (`struct.inner.x`).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub null_count: BTreeMap<String, u64>,
    /// The log's `tightBounds`, when it sets it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tight_bounds: Option<bool>,
}

/// One action from a commit file, in file order.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct LogAction {
    /// Action kind as stored (`add`, `remove`, `metaData`, …).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// Data path when the action names one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

/// Vended storage credentials. Non-sensitive for now, so a [`Table`] may carry
/// them; a later pass moves them off the wire.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Credentials {
    /// The access key id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_key_id: Option<String>,
    /// The secret access key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_access_key: Option<String>,
    /// The session token, when the lease is temporary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_token: Option<String>,
    /// When the lease expires, RFC-3339.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expire_time: Option<String>,
}

impl Credentials {
    /// The lease the `AWS_*` options a document carries name.
    #[must_use]
    pub fn from_env(env: &BTreeMap<String, String>) -> Self {
        Self {
            access_key_id: env.get("AWS_ACCESS_KEY_ID").cloned(),
            secret_access_key: env.get("AWS_SECRET_ACCESS_KEY").cloned(),
            session_token: env.get("AWS_SESSION_TOKEN").cloned(),
            expire_time: None,
        }
    }

    /// The `AWS_*` options to read storage with. Non-credential options
    /// (`AWS_REGION`, `AWS_ENDPOINT`, flags) are configuration and come from
    /// the process environment, not the wire.
    #[must_use]
    pub fn to_env(&self) -> BTreeMap<String, String> {
        [
            ("AWS_ACCESS_KEY_ID", &self.access_key_id),
            ("AWS_SECRET_ACCESS_KEY", &self.secret_access_key),
            ("AWS_SESSION_TOKEN", &self.session_token),
        ]
        .into_iter()
        .filter_map(|(key, value)| Some((key.to_string(), value.clone()?)))
        .collect()
    }

    /// Whether the lease names nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.access_key_id.is_none()
            && self.secret_access_key.is_none()
            && self.session_token.is_none()
            && self.expire_time.is_none()
    }
}

/// The result `credentials check` returns.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ReadEligibility {
    /// Whether the table is readable outside Databricks compute.
    #[serde(default)]
    pub eligible: bool,
    /// Why not, when it is not.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<EligibilityReason>,
}

/// One rewrite `experiment` measured: the codec settings and the resulting
/// size.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ExperimentTrial {
    /// The rewrites applied.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rewrites: Vec<String>,
    /// The resulting size in bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    /// The resulting compression ratio.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ratio: Option<f64>,
}

#[cfg(test)]
mod tests {
    use super::name;

    #[test]
    fn names_round_trip_through_their_parsers() {
        assert_eq!(
            name::parse_catalog(&name::catalog("dbx_samples")).as_deref(),
            Some("dbx_samples")
        );
        assert_eq!(
            name::parse_schema(&name::schema("dbx_samples", "nyctaxi")),
            Some(("dbx_samples".to_string(), "nyctaxi".to_string()))
        );
        assert_eq!(
            name::parse_table(&name::table("dbx_samples", "nyctaxi", "trips")),
            Some((
                "dbx_samples".to_string(),
                "nyctaxi".to_string(),
                "trips".to_string()
            ))
        );
        assert_eq!(
            name::parse_partition(&name::partition("c", "s", "t", "1")),
            Some((
                "c".to_string(),
                "s".to_string(),
                "t".to_string(),
                "1".to_string()
            ))
        );
    }

    #[test]
    fn an_id_with_a_slash_stays_one_segment() {
        let value = name::table("c", "s", "t/x");
        assert!(value.contains("%2F"), "{value}");
        assert_eq!(
            name::parse_table(&value),
            Some(("c".to_string(), "s".to_string(), "t/x".to_string()))
        );
    }

    #[test]
    fn a_name_of_another_shape_parses_as_none() {
        assert_eq!(name::parse_table("catalogs/dbx_samples"), None);
        assert_eq!(name::parse_schema("catalogs/dbx_samples"), None);
    }

    #[test]
    fn a_blob_name_round_trips_to_the_storage_uri() {
        for uri in [
            "s3://lakehouse/unity/events/part-0.parquet",
            "/tmp/x.parquet",
            "file:///tmp/y.parquet",
        ] {
            let blob = name::blob_name(uri);
            assert!(blob.starts_with("blobs/"), "{blob}");
            let read = name::blob_uri(&blob).unwrap();
            assert!(
                read == uri || uri.strip_prefix("file://") == Some(read.as_str()),
                "{uri} -> {blob} -> {read}"
            );
        }
        assert_eq!(
            name::parse_blob("blobs/s3/bucket%2Fkey"),
            Some(("s3".to_string(), "bucket/key".to_string()))
        );
    }

    #[test]
    fn a_path_read_table_gets_a_local_name() {
        assert_eq!(
            name::local_table("/tmp/lake/events"),
            "catalogs/local/schemas/local/tables/events"
        );
        assert_eq!(
            name::parse_table(&name::local_table("s3://bucket/events/")),
            Some((
                "local".to_string(),
                "local".to_string(),
                "events".to_string()
            ))
        );
    }

    #[test]
    fn a_table_file_name_parses_to_its_table() {
        let value = name::table_file("c", "s", "t", "part-0.parquet");
        assert_eq!(
            name::parse_table_file(&value),
            Some((
                "catalogs/c/schemas/s/tables/t".to_string(),
                "part-0.parquet".to_string()
            ))
        );
        assert_eq!(
            name::parse_table_file("catalogs/c/schemas/s/tables/t"),
            None
        );
    }
}
