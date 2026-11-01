//! Fold a bytemass stream into one row per table column.
//!
//! `list columns` measures files and writes `v1::Blob` and `v1::Column` lines.
//! This module does not read Parquet: it groups the measured columns by their
//! table and column path, weights each measurement by the inverse of its
//! `sample_rate`, and estimates the storage cost from the blobs' sizes. The
//! rows are a command document (no addressable name), not a resource.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::Deserialize;

use crate::v1;

/// One candidate monthly storage cost: a class with one price, or one access
/// sub-tier of a class that chooses its price by access.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StorageTierCost {
    /// The storage class, e.g. `STANDARD` or `INTELLIGENT_TIERING`.
    pub storage_class: String,
    /// The access sub-tier, for a class that prices by access.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub access_tier: Option<String>,
    /// The S3 list price this tier charges, in dollars per gibibyte per month.
    pub dollars_per_gibibyte_per_month: f64,
    /// The column's estimated monthly cost at this tier.
    pub cost_per_month: f64,
}

/// One table column's estimated mass over the files that measured it.
///
/// Each additive field is scaled by the inverse of the record's `sample_rate`,
/// so a `sample files` stream estimates the table rather than the sample.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ColumnMass {
    /// `catalogs/{catalog}/schemas/{schema}/tables/{table}`.
    pub table: String,
    /// The column path in schema form.
    pub column: String,
    /// Estimated on-disk (compressed) bytes.
    pub compressed_bytes: u64,
    /// Estimated encoded bytes before compression, when every measurement
    /// reported them.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uncompressed_bytes: Option<u64>,
    /// Estimated values in the column (including nulls).
    pub value_count: u64,
    /// [`Self::compressed_bytes`] divided by [`Self::value_count`].
    pub bytes_per_value: f64,
    /// Uncompressed bytes over compressed bytes, when every measurement
    /// reported both. Above 1 means the bytes did compress.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compression_ratio: Option<f64>,
    /// Estimated distinct files that measured the column.
    pub file_count: u64,
    /// The distinct storage classes behind those files, when known.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub storage_classes: Vec<String>,
    /// The candidate monthly storage costs, apportioned by the column's bytes
    /// in each class: one per class, and one per access sub-tier for a class
    /// that prices by access. A class whose files are not all priced is
    /// omitted, and the list is empty when no file is priced.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub storage_costs: Vec<StorageTierCost>,
}

impl ColumnMass {
    /// The row a document value carries, recognized by its shape: a table name,
    /// a column path, and the measured bytes. A value without those is not a
    /// summarized column.
    pub fn from_document(value: &serde_json::Value) -> Option<Self> {
        let row: Self = Self::deserialize(value).ok()?;
        crate::v1::Table::parse_name(&row.table)?;
        (!row.column.is_empty()).then_some(row)
    }
}

/// Errors summarizing a column stream.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// A record carried a `sample_rate` outside `(0, 1]`.
    SampleRate {
        /// The rate the record carried.
        sample_rate: f64,
    },
    /// A column named a table that is not a `catalogs/…/tables/…` name.
    TableName {
        /// The name the column carried.
        table: String,
    },
    /// A weighted total overflowed `u64` (or is not finite).
    Overflow {
        /// Which total overflowed.
        field: &'static str,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::SampleRate { sample_rate } => {
                write!(f, "a sample_rate must be in (0, 1], not {sample_rate}")
            }
            Error::TableName { table } => write!(
                f,
                "a column names `{table}`, not a catalogs/…/schemas/…/tables/… name"
            ),
            Error::Overflow { field } => write!(f, "the weighted {field} overflowed u64"),
        }
    }
}

impl std::error::Error for Error {}

/// Fold the measured columns into one row per table column, sorted by table
/// and then by column.
///
/// # Errors
/// Fails when a record carries a `sample_rate` outside `(0, 1]`, a column names
/// a table that is not canonical, or a weighted total does not fit in `u64`.
pub fn summarize_columns(
    columns: &[v1::Column],
    blobs: &[v1::Blob],
) -> Result<Vec<ColumnMass>, Error> {
    let mut facts: BTreeMap<&str, &v1::Blob> = BTreeMap::new();
    for blob in blobs {
        sample_weight(blob.sample_rate)?;
        facts.insert(blob.name.as_str(), blob);
    }
    let mut tables: BTreeMap<String, TableBytes> = BTreeMap::new();
    for column in columns {
        let Some((service, locator, path)) = v1::Column::parse_name(&column.name) else {
            continue;
        };
        let blob = v1::name::blob(&service, &locator);
        let Some(table) = resolve_table(column, &blob)? else {
            continue;
        };
        let mass = column.mass.as_ref();
        let weight = sample_weight(column.sample_rate)?;
        let bytes = weighted(
            mass.and_then(|mass| mass.compressed_bytes).unwrap_or(0),
            weight,
            "compressed_bytes",
        )?;
        let uncompressed = match mass.and_then(|mass| mass.uncompressed_bytes) {
            Some(bytes) => Some(weighted(bytes, weight, "uncompressed_bytes")?),
            None => None,
        };
        let values = weighted(
            mass.and_then(|mass| mass.value_count).unwrap_or(0),
            weight,
            "value_count",
        )?;
        let table = tables.entry(table).or_default();
        table.blobs.insert(blob.clone());
        let total = table.columns.entry(path).or_default();
        total.compressed_bytes += bytes;
        total.uncompressed.add(uncompressed);
        total.value_count += values;
        total.blobs.insert(blob.clone());
        if let Some(class) = blob_class(&blob, &facts) {
            *total.class_bytes.entry(class.clone()).or_default() += bytes;
            *table.class_bytes.entry(class).or_default() += bytes;
        }
    }

    let mut rows = Vec::with_capacity(columns.len());
    for (table, total) in tables {
        let costs = table_class_costs(&total.blobs, &facts)?;
        for (column, mass) in total.columns {
            rows.push(ColumnMass {
                table: table.clone(),
                column,
                compressed_bytes: round(mass.compressed_bytes, "compressed_bytes")?,
                uncompressed_bytes: (!mass.uncompressed.incomplete)
                    .then(|| round(mass.uncompressed.bytes, "uncompressed_bytes"))
                    .transpose()?,
                value_count: round(mass.value_count, "value_count")?,
                bytes_per_value: if mass.value_count > 0.0 {
                    mass.compressed_bytes / mass.value_count
                } else {
                    0.0
                },
                compression_ratio: mass.uncompressed.ratio(mass.compressed_bytes),
                file_count: round(count_files(&mass.blobs, &facts)?, "file_count")?,
                storage_classes: column_classes(&mass.blobs, &facts),
                storage_costs: column_costs(&mass.class_bytes, &total.class_bytes, &costs),
            });
        }
    }
    Ok(rows)
}

/// The number of records one kept record stands for: the inverse of its
/// `sample_rate`, one when unsampled.
fn sample_weight(sample_rate: Option<f64>) -> Result<f64, Error> {
    match sample_rate {
        None => Ok(1.0),
        Some(rate) if rate > 0.0 && rate <= 1.0 => {
            let weight = rate.recip();
            if weight.is_finite() {
                Ok(weight)
            } else {
                Err(Error::Overflow {
                    field: "sample_rate",
                })
            }
        }
        Some(sample_rate) => Err(Error::SampleRate { sample_rate }),
    }
}

/// One measurement scaled by its weight.
fn weighted(value: u64, weight: f64, field: &'static str) -> Result<f64, Error> {
    let value = weight * value as f64;
    if value.is_finite() {
        Ok(value)
    } else {
        Err(Error::Overflow { field })
    }
}

/// A summed f64 total that must fit in `u64`.
fn round(value: f64, field: &'static str) -> Result<u64, Error> {
    let rounded = value.round();
    if rounded.is_finite() && rounded < u64::MAX as f64 {
        Ok(rounded as u64)
    } else {
        Err(Error::Overflow { field })
    }
}

/// Uncompressed bytes behind a fold, and whether a measurement omitted them.
#[derive(Default)]
struct Uncompressed {
    bytes: f64,
    incomplete: bool,
}

impl Uncompressed {
    fn add(&mut self, bytes: Option<f64>) {
        match bytes {
            Some(bytes) => self.bytes += bytes,
            None => self.incomplete = true,
        }
    }

    /// The ratio when every measurement reported its bytes.
    fn ratio(&self, compressed_bytes: f64) -> Option<f64> {
        if self.incomplete {
            return None;
        }
        ratio(Some(self.bytes), compressed_bytes)
    }
}

/// Uncompressed over compressed, when both are known and compressed is not
/// zero.
fn ratio(uncompressed_bytes: Option<f64>, compressed_bytes: f64) -> Option<f64> {
    let uncompressed = uncompressed_bytes?;
    (compressed_bytes > 0.0).then(|| uncompressed / compressed_bytes)
}

/// What one column adds up to while a table is folded, weighted to the table.
#[derive(Default)]
struct ColumnBytes {
    compressed_bytes: f64,
    uncompressed: Uncompressed,
    value_count: f64,
    blobs: BTreeSet<String>,
    class_bytes: BTreeMap<String, f64>,
}

/// What one table's columns add up to while the rows are built.
#[derive(Default)]
struct TableBytes {
    columns: BTreeMap<String, ColumnBytes>,
    blobs: BTreeSet<String>,
    class_bytes: BTreeMap<String, f64>,
}

/// The files of one storage class, and whether every one of them has a size.
struct ClassFiles {
    gigabytes: f64,
    complete: bool,
}

/// The monthly cost of each class's files, by class. A class is absent when a
/// file of that class is not a priced S3 object (no size, unknown class).
fn table_class_costs(
    blobs: &BTreeSet<String>,
    facts: &BTreeMap<&str, &v1::Blob>,
) -> Result<BTreeMap<String, Vec<StorageTierCost>>, Error> {
    let mut files: BTreeMap<String, ClassFiles> = BTreeMap::new();
    for blob in blobs {
        let Some((service, _)) = v1::name::parse_blob(blob) else {
            continue;
        };
        // Only object storage has a monthly bill; a path on disk does not.
        if !object_storage(&service) {
            continue;
        }
        let Some(facts) = facts.get(blob.as_str()) else {
            continue;
        };
        // S3 omits the class of a STANDARD object; absent means STANDARD.
        let class = facts.storage_class.as_deref().unwrap_or("STANDARD");
        if storage_prices(class).is_none() {
            continue;
        }
        let total = files.entry(class.to_string()).or_insert(ClassFiles {
            gigabytes: 0.0,
            complete: true,
        });
        match facts.size_bytes {
            Some(size) => {
                let weight = sample_weight(facts.sample_rate)?;
                total.gigabytes += weight * size as f64 / GIBIBYTE;
                if !total.gigabytes.is_finite() {
                    return Err(Error::Overflow {
                        field: "size_bytes",
                    });
                }
            }
            None => total.complete = false,
        }
    }
    let mut costs = BTreeMap::new();
    for (class, files) in files {
        if !files.complete {
            continue;
        }
        let prices = storage_prices(&class).expect("the class was priced when it was recorded");
        costs.insert(
            class.clone(),
            prices
                .iter()
                .map(|(tier, price)| StorageTierCost {
                    storage_class: class.clone(),
                    access_tier: tier.map(str::to_string),
                    dollars_per_gibibyte_per_month: *price,
                    cost_per_month: files.gigabytes * price,
                })
                .collect(),
        );
    }
    Ok(costs)
}

/// A column's share of each class's cost, by the column's bytes in that class.
/// Each map holds compressed bytes per storage class.
fn column_costs(
    column: &BTreeMap<String, f64>,
    table: &BTreeMap<String, f64>,
    costs: &BTreeMap<String, Vec<StorageTierCost>>,
) -> Vec<StorageTierCost> {
    let mut out = Vec::new();
    for (class, bytes) in column {
        let Some(tiers) = costs.get(class) else {
            continue;
        };
        let total = table.get(class).copied().unwrap_or(0.0);
        if total <= 0.0 {
            continue;
        }
        let share = bytes / total;
        out.extend(tiers.iter().map(|tier| StorageTierCost {
            storage_class: tier.storage_class.clone(),
            access_tier: tier.access_tier.clone(),
            dollars_per_gibibyte_per_month: tier.dollars_per_gibibyte_per_month,
            cost_per_month: tier.cost_per_month * share,
        }));
    }
    out
}

/// The estimated number of distinct files behind a column: each measured file
/// counts for the inverse of its `sample_rate`.
fn count_files(blobs: &BTreeSet<String>, facts: &BTreeMap<&str, &v1::Blob>) -> Result<f64, Error> {
    let mut count = 0.0;
    for blob in blobs {
        let sample_rate = facts.get(blob.as_str()).and_then(|facts| facts.sample_rate);
        count += sample_weight(sample_rate)?;
    }
    if count.is_finite() {
        Ok(count)
    } else {
        Err(Error::Overflow {
            field: "file_count",
        })
    }
}

/// The distinct storage classes behind a column's files, sorted. A path on
/// disk has no class.
fn column_classes(blobs: &BTreeSet<String>, facts: &BTreeMap<&str, &v1::Blob>) -> Vec<String> {
    let mut classes = BTreeSet::new();
    for blob in blobs {
        if let Some(class) = blob_class(blob, facts) {
            classes.insert(class);
        }
    }
    classes.into_iter().collect()
}

/// A blob's storage class, when it is an object-storage file.
fn blob_class(blob: &str, facts: &BTreeMap<&str, &v1::Blob>) -> Option<String> {
    let (service, _) = v1::name::parse_blob(blob)?;
    if !object_storage(&service) {
        return None;
    }
    let facts = facts.get(blob)?;
    // S3 omits the class of a STANDARD object; absent means STANDARD.
    Some(
        facts
            .storage_class
            .as_deref()
            .unwrap_or("STANDARD")
            .to_string(),
    )
}

/// Whether a blob service is S3 object storage (the Hadoop-prefixed `s3a` too).
fn object_storage(service: &str) -> bool {
    matches!(service, "s3" | "s3a")
}

/// Bytes in a gibibyte. The list prices are quoted per GB-month; this uses the
/// binary gigabyte AWS bills against.
const GIBIBYTE: f64 = 1_073_741_824.0;

/// The S3 list prices per GiB-month (us-east-1) for a storage class, keyed by
/// access sub-tier: one entry for a class with a single price, five for
/// Intelligent-Tiering.
///
/// HEAD reports only the class, never the sub-tier, so a class that prices by
/// access returns every sub-tier (Frequent Access down to Deep Archive Access)
/// and the consumer picks. The sub-tier is chosen by how often the object is
/// read; the object monitoring fee is not included.
fn storage_prices(storage_class: &str) -> Option<&'static [(Option<&'static str>, f64)]> {
    Some(match storage_class {
        "STANDARD" => &[(None, 0.023)],
        "REDUCED_REDUNDANCY" => &[(None, 0.024)],
        "INTELLIGENT_TIERING" => &[
            (Some("FREQUENT_ACCESS"), 0.023),
            (Some("INFREQUENT_ACCESS"), 0.0125),
            (Some("ARCHIVE_INSTANT_ACCESS"), 0.004),
            (Some("ARCHIVE_ACCESS"), 0.0036),
            (Some("DEEP_ARCHIVE_ACCESS"), 0.00099),
        ],
        "STANDARD_IA" => &[(None, 0.0125)],
        "ONEZONE_IA" => &[(None, 0.01)],
        "EXPRESS_ONEZONE" => &[(None, 0.016)],
        "GLACIER_IR" => &[(None, 0.004)],
        "GLACIER" => &[(None, 0.0036)],
        "DEEP_ARCHIVE" => &[(None, 0.00099)],
        _ => return None,
    })
}

/// The table a measurement names: the table-file's table, or the local table a
/// bare path takes (the data model gives a path-read table `local.local`).
fn resolve_table(column: &v1::Column, blob: &str) -> Result<Option<String>, Error> {
    let table = match column.table.clone() {
        Some(table) => table,
        None => {
            let Some(uri) = v1::name::blob_uri(blob) else {
                return Ok(None);
            };
            v1::name::local_table(&uri)
        }
    };
    if v1::Table::parse_name(&table).is_none() {
        return Err(Error::TableName { table });
    }
    Ok(Some(table))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v1::name;

    fn blob(uri: &str) -> v1::Blob {
        v1::Blob {
            name: name::blob_name(uri),
            size_bytes: Some(40),
            storage_class: Some("STANDARD".into()),
            ..v1::Blob::default()
        }
    }

    fn column(
        file: &v1::Blob,
        table: Option<&str>,
        column: &str,
        bytes: u64,
        values: u64,
    ) -> v1::Column {
        let (service, locator) = name::parse_blob(&file.name).expect("a blob name");
        v1::Column {
            name: name::column(&service, &locator, column),
            table: table.map(str::to_string),
            sample_rate: file.sample_rate,
            mass: Some(v1::Bytemass {
                compressed_bytes: Some(bytes),
                uncompressed_bytes: Some(bytes * 3),
                value_count: Some(values),
                ..v1::Bytemass::default()
            }),
            ..v1::Column::default()
        }
    }

    const TABLE: &str = "catalogs/tpch/schemas/sf1/tables/lineitem";

    #[test]
    fn weights_a_sampled_column_to_the_table() {
        let file = v1::Blob {
            sample_rate: Some(0.25),
            ..blob("s3://lake/lineitem/part-0.parquet")
        };
        let rows =
            summarize_columns(&[column(&file, Some(TABLE), "l_comment", 8, 10)], &[file]).unwrap();

        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.table, TABLE);
        assert_eq!(row.column, "l_comment");
        assert_eq!(row.compressed_bytes, 32);
        assert_eq!(row.uncompressed_bytes, Some(96));
        assert_eq!(row.value_count, 40);
        assert_eq!(row.bytes_per_value, 0.8);
        assert_eq!(row.compression_ratio, Some(3.0));
        assert_eq!(row.file_count, 4);
        assert_eq!(row.storage_classes, vec!["STANDARD".to_string()]);
        let cost = &row.storage_costs[0];
        assert_eq!(cost.storage_class, "STANDARD");
        assert_eq!(cost.dollars_per_gibibyte_per_month, 0.023);
        let expected = 160.0 / GIBIBYTE * 0.023;
        assert!((cost.cost_per_month - expected).abs() < 1e-18);
    }

    #[test]
    fn weights_each_file_by_its_own_sample_rate() {
        let first = v1::Blob {
            sample_rate: Some(0.25),
            ..blob("s3://lake/lineitem/part-0.parquet")
        };
        let second = v1::Blob {
            sample_rate: Some(0.5),
            ..blob("s3://lake/lineitem/part-1.parquet")
        };
        let rows = summarize_columns(
            &[
                column(&first, Some(TABLE), "l_comment", 5, 10),
                column(&second, Some(TABLE), "l_comment", 10, 10),
            ],
            &[first, second],
        )
        .unwrap();

        let row = &rows[0];
        assert_eq!(row.compressed_bytes, 40);
        assert_eq!(row.value_count, 60);
        assert_eq!(row.file_count, 6);
    }

    #[test]
    fn prices_intelligent_tiering_by_access_sub_tier() {
        let mut file = blob("s3://lake/part-0.parquet");
        file.size_bytes = Some(GIBIBYTE as u64);
        file.storage_class = Some("INTELLIGENT_TIERING".into());
        let rows = summarize_columns(&[column(&file, Some(TABLE), "id", 4, 1)], &[file]).unwrap();

        let costs = &rows[0].storage_costs;
        assert_eq!(costs.len(), 5, "{costs:?}");
        let tiers: Vec<&str> = costs
            .iter()
            .map(|cost| cost.access_tier.as_deref().unwrap_or(""))
            .collect();
        assert_eq!(
            tiers,
            vec![
                "FREQUENT_ACCESS",
                "INFREQUENT_ACCESS",
                "ARCHIVE_INSTANT_ACCESS",
                "ARCHIVE_ACCESS",
                "DEEP_ARCHIVE_ACCESS",
            ]
        );
        for (cost, expected) in costs.iter().zip([0.023, 0.0125, 0.004, 0.0036, 0.00099]) {
            assert!((cost.dollars_per_gibibyte_per_month - expected).abs() < 1e-12);
        }
    }

    #[test]
    fn prices_each_column_in_its_own_storage_class() {
        let mut standard = blob("s3://lake/orders/part-0.parquet");
        standard.size_bytes = Some(2 * GIBIBYTE as u64);
        let mut glacier = blob("s3://lake/orders/part-1.parquet");
        glacier.size_bytes = Some(GIBIBYTE as u64);
        glacier.storage_class = Some("GLACIER".into());

        let rows = summarize_columns(
            &[
                column(&standard, Some(TABLE), "id", 10, 10),
                column(&glacier, Some(TABLE), "note", 10, 10),
            ],
            &[standard, glacier],
        )
        .unwrap();

        let id = rows.iter().find(|row| row.column == "id").unwrap();
        let note = rows.iter().find(|row| row.column == "note").unwrap();
        assert_eq!(id.storage_costs.len(), 1, "{:?}", id.storage_costs);
        assert_eq!(id.storage_costs[0].storage_class, "STANDARD");
        assert!((id.storage_costs[0].cost_per_month - 2.0 * 0.023).abs() < 1e-12);
        assert_eq!(note.storage_costs.len(), 1, "{:?}", note.storage_costs);
        assert_eq!(note.storage_costs[0].storage_class, "GLACIER");
        assert!((note.storage_costs[0].cost_per_month - 0.0036).abs() < 1e-12);
    }

    #[test]
    fn prices_s3a_like_s3() {
        let mut file = blob("s3a://lake/part-0.parquet");
        file.size_bytes = Some(GIBIBYTE as u64);
        let rows = summarize_columns(&[column(&file, Some(TABLE), "id", 4, 1)], &[file]).unwrap();

        assert_eq!(rows[0].storage_classes, vec!["STANDARD".to_string()]);
        assert_eq!(rows[0].storage_costs.len(), 1);
        assert!((rows[0].storage_costs[0].cost_per_month - 0.023).abs() < 1e-12);
    }

    #[test]
    fn leaves_a_class_unpriced_when_a_file_has_no_size() {
        let mut without_size = blob("s3://lake/orders/part-0.parquet");
        without_size.size_bytes = None;
        let mut glacier = blob("s3://lake/orders/part-1.parquet");
        glacier.size_bytes = Some(GIBIBYTE as u64);
        glacier.storage_class = Some("GLACIER".into());

        let rows = summarize_columns(
            &[
                column(&without_size, Some(TABLE), "id", 10, 10),
                column(&glacier, Some(TABLE), "note", 10, 10),
            ],
            &[without_size, glacier],
        )
        .unwrap();

        let id = rows.iter().find(|row| row.column == "id").unwrap();
        let note = rows.iter().find(|row| row.column == "note").unwrap();
        assert!(id.storage_costs.is_empty(), "{:?}", id.storage_costs);
        assert_eq!(note.storage_costs.len(), 1);
    }

    #[test]
    fn leaves_a_local_file_unpriced() {
        let file = blob("/lake/local.parquet");
        let rows = summarize_columns(
            &[column(&file, None, "id", 4, 1)],
            std::slice::from_ref(&file),
        )
        .unwrap();

        assert_eq!(
            rows[0].table,
            "catalogs/local/schemas/local/tables/local.parquet"
        );
        assert!(rows[0].storage_costs.is_empty());
        assert!(rows[0].storage_classes.is_empty());
    }

    #[test]
    fn rejects_a_sample_rate_outside_the_unit_interval() {
        let mut file = blob("/lake/part-0.parquet");
        file.sample_rate = Some(2.0);
        let error = summarize_columns(&[], &[file]).unwrap_err().to_string();
        assert!(error.contains("sample_rate"), "{error}");
    }

    #[test]
    fn rejects_a_table_name_that_is_not_canonical() {
        let file = blob("/lake/part-0.parquet");
        let error = summarize_columns(&[column(&file, Some("orders"), "id", 4, 1)], &[file])
            .unwrap_err()
            .to_string();
        assert!(error.contains("orders"), "{error}");
    }

    #[test]
    fn rejects_a_weighted_total_that_does_not_fit() {
        let mut file = blob("s3://lake/part-0.parquet");
        file.sample_rate = Some(1e-9);
        let mut column = column(&file, Some(TABLE), "id", u64::MAX / 4, 1);
        column.sample_rate = file.sample_rate;
        let error = summarize_columns(&[column], &[file])
            .unwrap_err()
            .to_string();
        assert!(error.contains("overflow"), "{error}");
    }

    #[test]
    fn rejects_a_weight_that_is_not_finite() {
        let mut file = blob("s3://lake/part-0.parquet");
        file.sample_rate = Some(1e-320);
        let error = summarize_columns(&[], &[file]).unwrap_err().to_string();
        assert!(error.contains("overflow"), "{error}");
    }

    #[test]
    fn reads_a_row_only_when_it_carries_the_measured_fields() {
        let minimal = serde_json::json!({
            "table": TABLE,
            "column": "id",
        });
        assert_eq!(ColumnMass::from_document(&minimal), None);

        let row = ColumnMass {
            table: TABLE.into(),
            column: "id".into(),
            compressed_bytes: 4,
            value_count: 1,
            bytes_per_value: 4.0,
            file_count: 1,
            ..ColumnMass::default()
        };
        let value = serde_json::to_value(&row).unwrap();
        assert_eq!(ColumnMass::from_document(&value), Some(row));
    }
}
