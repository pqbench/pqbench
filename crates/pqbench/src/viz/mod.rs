//! Fold bytemass columns into a catalog → schema → table → column page.
//!
//! `bytemass` measures files and writes `v1::Blob` and `v1::Bytemass` lines.
//! This module does not read Parquet. It groups the columns by the table their
//! measurement names (or by the file, for a bare path), folds the totals up
//! the catalog hierarchy, and draws one level at a time in a static page.

mod html;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::Path;

use crate::v1;

pub use html::render_html;

/// Errors writing the treemap page.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// There were no columns to draw.
    Empty,
    /// The columns could not be encoded into the page.
    Render(serde_json::Error),
    /// The page could not be written.
    Write {
        /// The path that could not be written.
        path: String,
        /// The underlying I/O failure.
        source: std::io::Error,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Empty => write!(f, "html needs bytemass columns"),
            Error::Render(_) => write!(f, "cannot encode the columns"),
            Error::Write { path, .. } => write!(f, "cannot write {path}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Render(error) => Some(error),
            Error::Write { source, .. } => Some(source),
            Error::Empty => None,
        }
    }
}

/// The page's hierarchy: catalogs, their schemas, tables, and columns.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct Report {
    /// Catalogs, sorted by id.
    pub catalogs: Vec<CatalogNode>,
}

/// A monthly storage cost estimate in US dollars: a range, because some
/// classes (Intelligent-Tiering) choose their price by access sub-tier.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize)]
pub struct CostEstimate {
    /// The lowest monthly cost.
    pub first: f64,
    /// The highest monthly cost.
    pub last: f64,
}

/// One catalog: the total over its schemas.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct CatalogNode {
    /// `catalogs/{catalog}`.
    pub name: String,
    /// The user-facing label: the catalog id.
    pub display_name: String,
    /// On-disk bytes over every measured table.
    pub compressed_bytes: u64,
    /// Uncompressed bytes over compressed bytes, when every measurement below
    /// reported both. Above 1 means the bytes did compress.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compression_ratio: Option<f64>,
    /// Estimated monthly storage cost, when every file is priced.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_cost_per_month: Option<CostEstimate>,
    /// Uncompressed bytes behind the node; kept to derive
    /// [`Self::compression_ratio`], not drawn.
    #[serde(skip)]
    pub uncompressed_bytes: Option<u64>,
    /// The catalog's schemas, sorted by id.
    pub schemas: Vec<SchemaNode>,
}

/// One schema: the total over its tables.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct SchemaNode {
    /// `catalogs/{catalog}/schemas/{schema}`.
    pub name: String,
    /// The user-facing label: the schema id.
    pub display_name: String,
    /// On-disk bytes over every measured table.
    pub compressed_bytes: u64,
    /// Uncompressed bytes over compressed bytes, when every measurement below
    /// reported both. Above 1 means the bytes did compress.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compression_ratio: Option<f64>,
    /// Estimated monthly storage cost, when every file is priced.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_cost_per_month: Option<CostEstimate>,
    /// Uncompressed bytes behind the node; kept to derive
    /// [`Self::compression_ratio`], not drawn.
    #[serde(skip)]
    pub uncompressed_bytes: Option<u64>,
    /// The schema's tables, sorted by id.
    pub tables: Vec<TableNode>,
}

/// One table: the total over its columns, its columns, and how many files fed
/// them. The files themselves are not drawn: a table can carry thousands, and
/// a treemap of them says less than the column total.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct TableNode {
    /// `catalogs/{catalog}/schemas/{schema}/tables/{table}`.
    pub name: String,
    /// The user-facing label: the table id.
    pub display_name: String,
    /// On-disk bytes over every measured column.
    pub compressed_bytes: u64,
    /// Uncompressed bytes over compressed bytes, when every measurement of the
    /// columns reported both. Above 1 means the bytes did compress.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compression_ratio: Option<f64>,
    /// Estimated monthly storage cost, when every file is priced.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_cost_per_month: Option<CostEstimate>,
    /// The distinct storage classes behind the files, when known.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub storage_classes: Vec<String>,
    /// The table's columns, sorted by path.
    pub columns: Vec<ColumnNode>,
    /// Distinct measured files behind the columns.
    pub file_count: u64,
}

/// One column's summed bytes.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct ColumnNode {
    /// Column path in schema form.
    pub column: String,
    /// On-disk bytes over every file that measured the column.
    pub compressed_bytes: u64,
    /// Values over every file that measured the column.
    pub value_count: u64,
    /// [`Self::compressed_bytes`] divided by [`Self::value_count`].
    pub bytes_per_value: f64,
    /// Uncompressed bytes over compressed bytes, when every measurement of the
    /// column reported both. A value above 1 means the bytes did compress.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compression_ratio: Option<f64>,
    /// The table's monthly storage cost, apportioned by
    /// [`Self::compressed_bytes`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_cost_per_month: Option<CostEstimate>,
}

/// Write `path.html` from the measured columns and the blobs they came from.
///
/// # Errors
/// Fails when there are no columns or the HTML page cannot be written.
pub fn write_report(
    prefix: &Path,
    columns: &[v1::Column],
    blobs: &[v1::Blob],
) -> Result<(), Error> {
    let report = fold(columns, blobs);
    if report.catalogs.is_empty() {
        return Err(Error::Empty);
    }
    let html_path = prefix.with_extension("html");
    let html = render_html(&report, &title(&report))?;
    std::fs::write(&html_path, html).map_err(|error| Error::Write {
        path: html_path.display().to_string(),
        source: error,
    })
}

/// Uncompressed bytes behind a fold, and whether a measurement omitted them.
#[derive(Default)]
struct Uncompressed {
    bytes: u64,
    incomplete: bool,
}

impl Uncompressed {
    fn add(&mut self, bytes: Option<u64>) {
        match bytes {
            Some(bytes) => self.bytes += bytes,
            None => self.incomplete = true,
        }
    }

    /// The ratio when every measurement reported its bytes.
    fn ratio(&self, compressed_bytes: u64) -> Option<f64> {
        if self.incomplete {
            return None;
        }
        ratio(Some(self.bytes), compressed_bytes)
    }
}

/// Uncompressed over compressed, when both are known and compressed is not
/// zero.
fn ratio(uncompressed_bytes: Option<u64>, compressed_bytes: u64) -> Option<f64> {
    let uncompressed = uncompressed_bytes?;
    (compressed_bytes > 0).then(|| uncompressed as f64 / compressed_bytes as f64)
}

/// Add one measurement's uncompressed bytes, or poison the total when it does
/// not report them.
fn add_uncompressed(total: &mut Option<u64>, bytes: Option<u64>) {
    *total = match (*total, bytes) {
        (Some(total), Some(bytes)) => Some(total + bytes),
        _ => None,
    };
}

/// What one column adds up to while a table is folded.
#[derive(Default)]
struct ColumnBytes {
    compressed_bytes: u64,
    uncompressed: Uncompressed,
    value_count: u64,
}

/// What one table's columns add up to while the report is built.
#[derive(Default)]
struct TableBytes {
    compressed_bytes: u64,
    uncompressed: Uncompressed,
    columns: BTreeMap<String, ColumnBytes>,
    blobs: BTreeSet<String>,
}

/// Group the columns by their table and fold the totals up the hierarchy.
fn fold(columns: &[v1::Column], blobs: &[v1::Blob]) -> Report {
    let facts: BTreeMap<&str, &v1::Blob> = blobs
        .iter()
        .map(|blob| (blob.name.as_str(), blob))
        .collect();
    let mut tables: BTreeMap<(String, String, String), TableBytes> = BTreeMap::new();
    for column in columns {
        let Some((service, locator, path)) = v1::Column::parse_name(&column.name) else {
            continue;
        };
        let blob = v1::name::blob(&service, &locator);
        let Some(table) = resolve_table(column, &blob) else {
            continue;
        };
        let Some((catalog, schema, name)) = v1::Table::parse_name(&table) else {
            continue;
        };
        let mass = column.mass.as_ref();
        let bytes = mass.and_then(|mass| mass.compressed_bytes).unwrap_or(0);
        let values = mass.and_then(|mass| mass.value_count).unwrap_or(0);
        let table = tables.entry((catalog, schema, name)).or_default();
        table.compressed_bytes += bytes;
        table
            .uncompressed
            .add(mass.and_then(|mass| mass.uncompressed_bytes));
        table.blobs.insert(blob);
        let total = table.columns.entry(path).or_default();
        total.compressed_bytes += bytes;
        total
            .uncompressed
            .add(mass.and_then(|mass| mass.uncompressed_bytes));
        total.value_count += values;
    }

    let mut report = Report::default();
    for ((catalog, schema, name), table) in tables {
        let bytes = table.compressed_bytes;
        let (storage_cost_per_month, storage_classes) = table_cost(&table.blobs, &facts);
        let mut columns: Vec<ColumnNode> = table
            .columns
            .into_iter()
            .map(|(column, total)| ColumnNode {
                column,
                compressed_bytes: total.compressed_bytes,
                value_count: total.value_count,
                bytes_per_value: if total.value_count == 0 {
                    0.0
                } else {
                    total.compressed_bytes as f64 / total.value_count as f64
                },
                compression_ratio: total.uncompressed.ratio(total.compressed_bytes),
                storage_cost_per_month: None,
            })
            .collect();
        if let Some(cost) = storage_cost_per_month {
            if bytes > 0 {
                for column in &mut columns {
                    let share = column.compressed_bytes as f64 / bytes as f64;
                    column.storage_cost_per_month = Some(CostEstimate {
                        first: cost.first * share,
                        last: cost.last * share,
                    });
                }
            }
        }
        let uncompressed_bytes =
            (!table.uncompressed.incomplete).then_some(table.uncompressed.bytes);
        let node = TableNode {
            name: v1::name::table(&catalog, &schema, &name),
            display_name: name,
            compressed_bytes: bytes,
            compression_ratio: table.uncompressed.ratio(bytes),
            storage_cost_per_month,
            storage_classes,
            columns,
            file_count: table.blobs.len() as u64,
        };
        let catalog_node = ensure_catalog(&mut report, &catalog, uncompressed_bytes);
        catalog_node.compressed_bytes += bytes;
        let schema_node = ensure_schema(catalog_node, &schema, uncompressed_bytes);
        schema_node.compressed_bytes += bytes;
        schema_node.tables.push(node);
    }
    for catalog in &mut report.catalogs {
        for schema in &mut catalog.schemas {
            schema.compression_ratio = ratio(schema.uncompressed_bytes, schema.compressed_bytes);
            schema.storage_cost_per_month = sum_costs(
                schema
                    .tables
                    .iter()
                    .map(|table| table.storage_cost_per_month),
            );
        }
        catalog.compression_ratio = ratio(catalog.uncompressed_bytes, catalog.compressed_bytes);
        catalog.storage_cost_per_month = sum_costs(
            catalog
                .schemas
                .iter()
                .map(|schema| schema.storage_cost_per_month),
        );
    }
    report
}

/// The monthly S3 list-price cost of a table's files, and the distinct classes
/// behind them. `None` when a file is not a priced S3 object (a local file, an
/// unknown class, or a blob the stream did not describe).
fn table_cost(
    blobs: &BTreeSet<String>,
    facts: &BTreeMap<&str, &v1::Blob>,
) -> (Option<CostEstimate>, Vec<String>) {
    let mut first = 0.0;
    let mut last = 0.0;
    let mut priced = true;
    let mut classes = BTreeSet::new();
    for blob in blobs {
        let Some((service, _)) = v1::name::parse_blob(blob) else {
            priced = false;
            continue;
        };
        // Only object storage has a monthly bill; a path on disk does not.
        if service != "s3" {
            priced = false;
            continue;
        }
        let Some(facts) = facts.get(blob.as_str()) else {
            priced = false;
            continue;
        };
        let Some(size) = facts.size_bytes else {
            priced = false;
            continue;
        };
        // S3 omits the class of a STANDARD object; absent means STANDARD.
        let class = facts.storage_class.as_deref().unwrap_or("STANDARD");
        let Some((low, high)) = storage_price(class) else {
            priced = false;
            continue;
        };
        let gigabytes = size as f64 / GIBIBYTE;
        first += gigabytes * low;
        last += gigabytes * high;
        classes.insert(class.to_string());
    }
    let cost = (priced && !blobs.is_empty()).then_some(CostEstimate { first, last });
    (cost, classes.into_iter().collect())
}

/// Bytes in a gibibyte. The list prices are quoted per GB-month; this uses the
/// binary gigabyte AWS bills against.
const GIBIBYTE: f64 = 1_073_741_824.0;

/// The S3 list price range per GiB-month (us-east-1) for a storage class.
///
/// Intelligent-Tiering bills by access sub-tier (Frequent Access down to Deep
/// Archive Access), and HEAD reports only the class, not the sub-tier, so it
/// is the one class with a range. The sub-tier is chosen by how often the
/// object is read; the object monitoring fee is not included.
fn storage_price(storage_class: &str) -> Option<(f64, f64)> {
    Some(match storage_class {
        "STANDARD" => (0.023, 0.023),
        "REDUCED_REDUNDANCY" => (0.024, 0.024),
        "INTELLIGENT_TIERING" => (0.00099, 0.023),
        "STANDARD_IA" => (0.0125, 0.0125),
        "ONEZONE_IA" => (0.01, 0.01),
        "EXPRESS_ONEZONE" => (0.016, 0.016),
        "GLACIER_IR" => (0.004, 0.004),
        "GLACIER" => (0.0036, 0.0036),
        "DEEP_ARCHIVE" => (0.00099, 0.00099),
        _ => return None,
    })
}

/// Add child costs; `None` when any child is unpriced.
fn sum_costs(costs: impl Iterator<Item = Option<CostEstimate>>) -> Option<CostEstimate> {
    let mut first = 0.0;
    let mut last = 0.0;
    let mut any = false;
    for cost in costs {
        let cost = cost?;
        first += cost.first;
        last += cost.last;
        any = true;
    }
    any.then_some(CostEstimate { first, last })
}

/// The table a measurement names: the table-file's table, or the local table a
/// bare path takes (the data model gives a path-read table `local.local`).
fn resolve_table(column: &v1::Column, blob: &str) -> Option<String> {
    column.table.clone().or_else(|| {
        let uri = v1::name::blob_uri(blob)?;
        Some(v1::name::local_table(&uri))
    })
}

/// The catalog node, appending it when the fold reaches a new catalog.
fn ensure_catalog<'a>(
    report: &'a mut Report,
    catalog: &str,
    uncompressed_bytes: Option<u64>,
) -> &'a mut CatalogNode {
    let name = v1::name::catalog(catalog);
    if report.catalogs.last().map(|node| node.name.as_str()) != Some(name.as_str()) {
        report.catalogs.push(CatalogNode {
            name,
            display_name: catalog.to_string(),
            uncompressed_bytes: Some(0),
            ..CatalogNode::default()
        });
    }
    let node = report
        .catalogs
        .last_mut()
        .expect("a catalog was just pushed");
    add_uncompressed(&mut node.uncompressed_bytes, uncompressed_bytes);
    node
}

/// The schema node, appending it when the fold reaches a new schema.
fn ensure_schema<'a>(
    catalog: &'a mut CatalogNode,
    schema: &str,
    uncompressed_bytes: Option<u64>,
) -> &'a mut SchemaNode {
    let name = v1::name::schema(&catalog.display_name, schema);
    if catalog.schemas.last().map(|node| node.name.as_str()) != Some(name.as_str()) {
        catalog.schemas.push(SchemaNode {
            name,
            display_name: schema.to_string(),
            uncompressed_bytes: Some(0),
            ..SchemaNode::default()
        });
    }
    let node = catalog
        .schemas
        .last_mut()
        .expect("a schema was just pushed");
    add_uncompressed(&mut node.uncompressed_bytes, uncompressed_bytes);
    node
}

/// The page title: one table's dotted name, or how many tables were measured.
fn title(report: &Report) -> String {
    let mut tables = report
        .catalogs
        .iter()
        .flat_map(|catalog| catalog.schemas.iter().map(move |schema| (catalog, schema)))
        .flat_map(|(catalog, schema)| {
            schema
                .tables
                .iter()
                .map(move |table| (catalog, schema, table))
        });
    match (tables.next(), tables.next()) {
        (None, _) => "bytemass".into(),
        (Some((catalog, schema, table)), None) => format!(
            "{}.{}.{}",
            catalog.display_name, schema.display_name, table.display_name
        ),
        (Some(_), Some(_)) => {
            let count = report
                .catalogs
                .iter()
                .flat_map(|catalog| &catalog.schemas)
                .map(|schema| schema.tables.len())
                .sum::<usize>();
            format!("{count} tables")
        }
    }
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

    fn column(file: &v1::Blob, table: Option<&str>, column: &str, bytes: u64) -> v1::Column {
        let (service, locator) = name::parse_blob(&file.name).expect("a blob name");
        v1::Column {
            name: name::column(&service, &locator, column),
            table: table.map(str::to_string),
            mass: Some(v1::Bytemass {
                compressed_bytes: Some(bytes),
                value_count: Some(10),
                ..v1::Bytemass::default()
            }),
            ..v1::Column::default()
        }
    }

    fn measured(
        file: &v1::Blob,
        table: Option<&str>,
        path: &str,
        compressed_bytes: u64,
        uncompressed_bytes: u64,
    ) -> v1::Column {
        let mut column = column(file, table, path, compressed_bytes);
        column
            .mass
            .as_mut()
            .expect("the helper sets a mass")
            .uncompressed_bytes = Some(uncompressed_bytes);
        column
    }

    #[test]
    fn folds_columns_up_the_catalog_hierarchy() {
        let sales = blob("/lake/sales/part-0.parquet");
        let web = blob("/lake/web/part-0.parquet");
        let columns = vec![
            column(
                &sales,
                Some("catalogs/shop/schemas/sales/tables/orders"),
                "id",
                30,
            ),
            column(
                &sales,
                Some("catalogs/shop/schemas/sales/tables/orders"),
                "note",
                70,
            ),
            column(
                &web,
                Some("catalogs/shop/schemas/web/tables/clicks"),
                "id",
                25,
            ),
            column(
                &web,
                Some("catalogs/shop/schemas/web/tables/visits"),
                "id",
                5,
            ),
        ];
        let report = fold(&columns, &[sales, web]);

        assert_eq!(report.catalogs.len(), 1);
        let catalog = &report.catalogs[0];
        assert_eq!(catalog.display_name, "shop");
        assert_eq!(catalog.compressed_bytes, 130);
        assert_eq!(catalog.schemas.len(), 2);
        assert_eq!(catalog.schemas[0].display_name, "sales");
        assert_eq!(catalog.schemas[0].compressed_bytes, 100);
        assert_eq!(catalog.schemas[1].display_name, "web");
        assert_eq!(catalog.schemas[1].compressed_bytes, 30);

        let orders = &catalog.schemas[0].tables[0];
        assert_eq!(orders.display_name, "orders");
        assert_eq!(orders.compressed_bytes, 100);
        assert_eq!(orders.columns.len(), 2);
        assert_eq!(orders.columns[0].column, "id");
        assert_eq!(orders.columns[1].bytes_per_value, 7.0);
        assert_eq!(orders.file_count, 1);
    }

    #[test]
    fn folds_a_column_measured_in_several_files_once() {
        let first = blob("/lake/lineitem/part-0.parquet");
        let second = blob("/lake/lineitem/part-1.parquet");
        let table = "catalogs/tpch/schemas/sf1/tables/lineitem";
        let report = fold(
            &[
                column(&first, Some(table), "l_comment", 60),
                column(&second, Some(table), "l_comment", 40),
            ],
            &[first, second],
        );

        let node = &report.catalogs[0].schemas[0].tables[0];
        assert_eq!(node.file_count, 2);
        assert_eq!(node.columns.len(), 1);
        assert_eq!(node.columns[0].column, "l_comment");
        assert_eq!(node.columns[0].compressed_bytes, 100);
        assert_eq!(node.columns[0].value_count, 20);
        assert_eq!(node.columns[0].bytes_per_value, 5.0);
    }

    #[test]
    fn folds_the_compression_ratio_over_the_column_files() {
        let first = blob("/lake/lineitem/part-0.parquet");
        let second = blob("/lake/lineitem/part-1.parquet");
        let table = "catalogs/tpch/schemas/sf1/tables/lineitem";
        let report = fold(
            &[
                measured(&first, Some(table), "l_comment", 8, 24),
                measured(&second, Some(table), "l_comment", 4, 12),
            ],
            &[first, second],
        );

        let catalog = &report.catalogs[0];
        assert_eq!(catalog.compression_ratio, Some(3.0));
        assert_eq!(catalog.schemas[0].compression_ratio, Some(3.0));
        let table = &catalog.schemas[0].tables[0];
        assert_eq!(table.compression_ratio, Some(3.0));
        assert_eq!(table.columns[0].compression_ratio, Some(3.0));
    }

    #[test]
    fn weights_the_parents_ratio_by_the_child_bytes() {
        let orders = blob("/lake/orders/part-0.parquet");
        let items = blob("/lake/items/part-0.parquet");
        let report = fold(
            &[
                measured(
                    &orders,
                    Some("catalogs/shop/schemas/sales/tables/orders"),
                    "id",
                    10,
                    20,
                ),
                measured(
                    &items,
                    Some("catalogs/shop/schemas/sales/tables/items"),
                    "id",
                    30,
                    30,
                ),
            ],
            &[orders, items],
        );

        let catalog = &report.catalogs[0];
        assert_eq!(catalog.schemas[0].compression_ratio, Some(1.25));
        assert_eq!(catalog.compression_ratio, Some(1.25));
    }

    #[test]
    fn leaves_the_ratio_unknown_when_a_measurement_omits_uncompressed_bytes() {
        let file = blob("/lake/lineitem/part-0.parquet");
        let table = "catalogs/tpch/schemas/sf1/tables/lineitem";
        let report = fold(
            &[
                measured(&file, Some(table), "l_comment", 8, 24),
                column(&file, Some(table), "l_comment", 4),
            ],
            std::slice::from_ref(&file),
        );

        let catalog = &report.catalogs[0];
        assert_eq!(catalog.compression_ratio, None);
        assert_eq!(catalog.schemas[0].compression_ratio, None);
        assert_eq!(catalog.schemas[0].tables[0].compression_ratio, None);
        assert_eq!(
            catalog.schemas[0].tables[0].columns[0].compression_ratio,
            None
        );
    }

    #[test]
    fn prices_a_standard_s3_file_and_leaves_a_local_path_unpriced() {
        let table = "catalogs/shop/schemas/sales/tables/orders";
        let s3 = blob("s3://lake/orders/part-0.parquet");
        let report = fold(
            &[column(&s3, Some(table), "id", 4)],
            std::slice::from_ref(&s3),
        );
        let node = &report.catalogs[0].schemas[0].tables[0];
        let expected = 40.0 / GIBIBYTE * 0.023;
        let cost = node.storage_cost_per_month.expect("a priced file");
        assert!((cost.first - expected).abs() < 1e-15);
        assert!((cost.last - expected).abs() < 1e-15);
        assert_eq!(node.storage_classes, vec!["STANDARD".to_string()]);
        assert_eq!(node.columns[0].storage_cost_per_month, Some(cost));
        assert_eq!(
            report.catalogs[0].schemas[0].storage_cost_per_month,
            Some(cost)
        );
        assert_eq!(report.catalogs[0].storage_cost_per_month, Some(cost));

        let local = blob("/lake/local.parquet");
        let report = fold(
            &[column(&local, None, "id", 4)],
            std::slice::from_ref(&local),
        );
        let node = &report.catalogs[0].schemas[0].tables[0];
        assert!(node.storage_cost_per_month.is_none());
        assert!(node.storage_classes.is_empty());
        assert!(report.catalogs[0].storage_cost_per_month.is_none());
    }

    #[test]
    fn ranges_intelligent_tiering_over_its_sub_tiers() {
        let mut file = blob("s3://lake/part-0.parquet");
        file.size_bytes = Some(GIBIBYTE as u64);
        file.storage_class = Some("INTELLIGENT_TIERING".into());
        let table = "catalogs/shop/schemas/sales/tables/orders";
        let report = fold(
            &[column(&file, Some(table), "id", 4)],
            std::slice::from_ref(&file),
        );

        let node = &report.catalogs[0].schemas[0].tables[0];
        let cost = node.storage_cost_per_month.expect("a priced file");
        assert!((cost.first - 0.00099).abs() < 1e-12, "{cost:?}");
        assert!((cost.last - 0.023).abs() < 1e-12, "{cost:?}");
        assert_eq!(
            node.storage_classes,
            vec!["INTELLIGENT_TIERING".to_string()]
        );
    }

    #[test]
    fn a_bare_file_takes_the_local_table() {
        let file = blob("/lake/quickstart.parquet");
        let report = fold(
            &[column(&file, None, "text", 8)],
            std::slice::from_ref(&file),
        );

        let catalog = &report.catalogs[0];
        assert_eq!(catalog.display_name, "local");
        assert_eq!(catalog.schemas[0].display_name, "local");
        assert_eq!(
            catalog.schemas[0].tables[0].display_name,
            "quickstart.parquet"
        );
        assert_eq!(title(&report), "local.local.quickstart.parquet");
    }

    #[test]
    fn report_writes_html() {
        let file = blob("/tmp/a<b>.parquet");
        let directory = tempfile::tempdir().unwrap();
        let prefix = directory.path().join("masses");
        write_report(
            &prefix,
            &[column(&file, None, "text", 4)],
            std::slice::from_ref(&file),
        )
        .unwrap();

        let html = std::fs::read_to_string(prefix.with_extension("html")).unwrap();
        assert!(html.starts_with("<!DOCTYPE html>"));
        assert!(html.contains("d3-hierarchy@3"));
        assert!(html.contains("a&lt;b&gt;.parquet"));
        assert!(!html.contains("a<b>.parquet"));
        assert!(!html.contains("</script><script"));
    }

    #[test]
    fn empty_columns_fail() {
        let directory = tempfile::tempdir().unwrap();
        let error = write_report(&directory.path().join("empty"), &[], &[])
            .unwrap_err()
            .to_string();
        assert!(error.contains("bytemass columns"), "{error}");
    }
}
