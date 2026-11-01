//! Draw a `summarize columns` document as a catalog → schema → table → column
//! treemap.
//!
//! `summarize columns` folds the bytemass stream into one document row per
//! table column, already weighted to the table. This module does not aggregate:
//! it groups the rows by the table their name carries, folds the totals up the
//! catalog hierarchy, and draws one level at a time in a static page.

mod html;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::Path;

use crate::mass::{self, ColumnMass};
use crate::v1;

pub use html::render_html;

/// Errors writing the treemap page.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// There were no rows to draw.
    Empty,
    /// The rows could not be encoded into the page.
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
            Error::Empty => write!(f, "html needs summarized columns"),
            Error::Render(_) => write!(f, "cannot encode the rows"),
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
    pub uncompressed_bytes: Option<f64>,
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
    pub uncompressed_bytes: Option<f64>,
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
    /// Distinct measured files behind the columns; the largest per-column
    /// count when they differ.
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

/// Render the rows `summarize columns` produced as the HTML page.
///
/// # Errors
/// Fails when there are no rows or the page cannot be encoded.
pub fn render_rows(rows: &[ColumnMass]) -> Result<String, Error> {
    let report = fold(rows);
    if report.catalogs.is_empty() {
        return Err(Error::Empty);
    }
    render_html(&report, &title(&report))
}

/// Write `path.html` from the rows `summarize columns` produced.
///
/// # Errors
/// Fails when there are no rows or the HTML page cannot be written.
pub fn write_report(prefix: &Path, rows: &[ColumnMass]) -> Result<(), Error> {
    let html = render_rows(rows)?;
    let html_path = prefix.with_extension("html");
    std::fs::write(&html_path, html).map_err(|error| Error::Write {
        path: html_path.display().to_string(),
        source: error,
    })
}

/// Uncompressed bytes behind a fold, and whether a row omitted them.
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

    /// The ratio when every row reported its bytes.
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

/// A sum of costs that stays `None` when any part is unpriced.
#[derive(Debug, Clone, Copy)]
struct Costs {
    first: f64,
    last: f64,
    priced: bool,
    any: bool,
}

impl Default for Costs {
    fn default() -> Self {
        Self {
            first: 0.0,
            last: 0.0,
            priced: true,
            any: false,
        }
    }
}

impl Costs {
    fn add(&mut self, cost: Option<CostEstimate>) {
        match cost {
            Some(cost) => {
                self.first += cost.first;
                self.last += cost.last;
                self.any = true;
            }
            None => self.priced = false,
        }
    }

    fn total(self) -> Option<CostEstimate> {
        (self.priced && self.any).then_some(CostEstimate {
            first: self.first,
            last: self.last,
        })
    }
}

/// The first and last of a row's tier costs, when it is priced.
fn cost_estimate(tiers: &[mass::StorageTierCost]) -> Option<CostEstimate> {
    if tiers.is_empty() {
        return None;
    }
    let first = tiers
        .iter()
        .map(|tier| tier.cost_per_month)
        .fold(f64::INFINITY, f64::min);
    let last = tiers
        .iter()
        .map(|tier| tier.cost_per_month)
        .fold(f64::NEG_INFINITY, f64::max);
    Some(CostEstimate { first, last })
}

/// What one table's rows add up to while the report is built.
#[derive(Default)]
struct TableRows {
    compressed_bytes: u64,
    uncompressed: Uncompressed,
    file_count: u64,
    classes: BTreeSet<String>,
    cost: Costs,
    columns: Vec<ColumnNode>,
}

/// What one schema's tables add up to while the report is built.
#[derive(Default)]
struct SchemaRows {
    compressed_bytes: u64,
    uncompressed: Uncompressed,
    cost: Costs,
    tables: Vec<TableNode>,
}

/// What one catalog's schemas add up to while the report is built.
#[derive(Default)]
struct CatalogRows {
    compressed_bytes: u64,
    uncompressed: Uncompressed,
    cost: Costs,
    schemas: BTreeMap<String, SchemaRows>,
}

/// Group the rows by their table and fold the totals up the hierarchy.
fn fold(rows: &[ColumnMass]) -> Report {
    let mut tables: BTreeMap<(String, String, String), TableRows> = BTreeMap::new();
    for row in rows {
        let Some((catalog, schema, name)) = v1::Table::parse_name(&row.table) else {
            continue;
        };
        let table = tables.entry((catalog, schema, name)).or_default();
        table.compressed_bytes += row.compressed_bytes;
        table
            .uncompressed
            .add(row.uncompressed_bytes.map(|bytes| bytes as f64));
        table.file_count = table.file_count.max(row.file_count);
        table.classes.extend(row.storage_classes.iter().cloned());
        let cost = cost_estimate(&row.storage_costs);
        table.cost.add(cost);
        table.columns.push(ColumnNode {
            column: row.column.clone(),
            compressed_bytes: row.compressed_bytes,
            value_count: row.value_count,
            bytes_per_value: row.bytes_per_value,
            compression_ratio: row.compression_ratio,
            storage_cost_per_month: cost,
        });
    }

    let mut catalogs: BTreeMap<String, CatalogRows> = BTreeMap::new();
    for ((catalog, schema, name), mut table) in tables {
        table
            .columns
            .sort_by(|left, right| left.column.cmp(&right.column));
        let compressed_bytes = table.compressed_bytes;
        let uncompressed_bytes =
            (!table.uncompressed.incomplete).then_some(table.uncompressed.bytes);
        let node = TableNode {
            name: v1::name::table(&catalog, &schema, &name),
            display_name: name,
            compressed_bytes,
            compression_ratio: table.uncompressed.ratio(compressed_bytes as f64),
            storage_cost_per_month: table.cost.total(),
            storage_classes: table.classes.into_iter().collect(),
            columns: table.columns,
            file_count: table.file_count,
        };
        let catalog_rows = catalogs.entry(catalog.clone()).or_default();
        catalog_rows.compressed_bytes += compressed_bytes;
        catalog_rows.uncompressed.add(uncompressed_bytes);
        catalog_rows.cost.add(node.storage_cost_per_month);
        let schema_rows = catalog_rows.schemas.entry(schema).or_default();
        schema_rows.compressed_bytes += compressed_bytes;
        schema_rows.uncompressed.add(uncompressed_bytes);
        schema_rows.cost.add(node.storage_cost_per_month);
        schema_rows.tables.push(node);
    }

    let mut report = Report::default();
    for (catalog, rows) in catalogs {
        let uncompressed_bytes = (!rows.uncompressed.incomplete).then_some(rows.uncompressed.bytes);
        let mut node = CatalogNode {
            name: v1::name::catalog(&catalog),
            display_name: catalog.clone(),
            compressed_bytes: rows.compressed_bytes,
            compression_ratio: ratio(uncompressed_bytes, rows.compressed_bytes as f64),
            storage_cost_per_month: rows.cost.total(),
            uncompressed_bytes,
            schemas: Vec::with_capacity(rows.schemas.len()),
        };
        for (schema, rows) in rows.schemas {
            let uncompressed_bytes =
                (!rows.uncompressed.incomplete).then_some(rows.uncompressed.bytes);
            node.schemas.push(SchemaNode {
                name: v1::name::schema(&catalog, &schema),
                display_name: schema,
                compressed_bytes: rows.compressed_bytes,
                compression_ratio: ratio(uncompressed_bytes, rows.compressed_bytes as f64),
                storage_cost_per_month: rows.cost.total(),
                uncompressed_bytes,
                tables: rows.tables,
            });
        }
        report.catalogs.push(node);
    }
    report
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
    use crate::mass::StorageTierCost;

    fn row(table: &str, column: &str, bytes: u64, values: u64) -> ColumnMass {
        ColumnMass {
            table: table.to_string(),
            column: column.to_string(),
            compressed_bytes: bytes,
            uncompressed_bytes: Some(bytes * 3),
            value_count: values,
            bytes_per_value: if values == 0 {
                0.0
            } else {
                bytes as f64 / values as f64
            },
            compression_ratio: Some(3.0),
            file_count: 1,
            storage_classes: vec!["STANDARD".into()],
            storage_costs: vec![StorageTierCost {
                storage_class: "STANDARD".into(),
                access_tier: None,
                dollars_per_gibibyte_per_month: 0.023,
                cost_per_month: 2.0,
            }],
        }
    }

    #[test]
    fn folds_rows_up_the_catalog_hierarchy() {
        let report = fold(&[
            row("catalogs/shop/schemas/sales/tables/orders", "id", 30, 10),
            row("catalogs/shop/schemas/sales/tables/orders", "note", 70, 10),
            row("catalogs/shop/schemas/web/tables/clicks", "id", 25, 10),
        ]);

        assert_eq!(report.catalogs.len(), 1);
        let catalog = &report.catalogs[0];
        assert_eq!(catalog.display_name, "shop");
        assert_eq!(catalog.compressed_bytes, 125);
        assert_eq!(catalog.schemas.len(), 2);
        assert_eq!(catalog.schemas[0].display_name, "sales");
        assert_eq!(catalog.schemas[0].compressed_bytes, 100);
        assert_eq!(catalog.schemas[1].compressed_bytes, 25);

        let orders = &catalog.schemas[0].tables[0];
        assert_eq!(orders.display_name, "orders");
        assert_eq!(orders.compressed_bytes, 100);
        assert_eq!(orders.columns.len(), 2);
        assert_eq!(orders.columns[0].column, "id");
        assert_eq!(orders.columns[1].bytes_per_value, 7.0);
        assert_eq!(title(&report), "2 tables");
    }

    #[test]
    fn sums_the_ratio_and_cost_of_a_tables_rows() {
        let report = fold(&[
            row("catalogs/shop/schemas/sales/tables/orders", "id", 10, 10),
            row("catalogs/shop/schemas/sales/tables/orders", "note", 30, 10),
        ]);

        let catalog = &report.catalogs[0];
        let table = &catalog.schemas[0].tables[0];
        assert_eq!(table.compressed_bytes, 40);
        assert_eq!(table.compression_ratio, Some(3.0));
        assert_eq!(
            table.storage_cost_per_month,
            Some(CostEstimate {
                first: 4.0,
                last: 4.0
            })
        );
        assert_eq!(catalog.storage_cost_per_month, table.storage_cost_per_month);
        assert_eq!(title(&report), "shop.sales.orders");
    }

    #[test]
    fn leaves_the_ratio_unknown_when_a_row_omits_uncompressed_bytes() {
        let mut missing = row("catalogs/shop/schemas/sales/tables/orders", "id", 10, 10);
        missing.uncompressed_bytes = None;
        let report = fold(&[
            row("catalogs/shop/schemas/sales/tables/orders", "note", 30, 10),
            missing,
        ]);

        let catalog = &report.catalogs[0];
        assert_eq!(catalog.compression_ratio, None);
        assert_eq!(catalog.schemas[0].compression_ratio, None);
        assert_eq!(catalog.schemas[0].tables[0].compression_ratio, None);
    }

    #[test]
    fn leaves_the_cost_unknown_when_a_row_is_unpriced() {
        let mut unpriced = row("catalogs/shop/schemas/sales/tables/orders", "id", 10, 10);
        unpriced.storage_costs.clear();
        let report = fold(&[unpriced]);

        assert_eq!(report.catalogs[0].storage_cost_per_month, None);
        assert_eq!(report.catalogs[0].schemas[0].storage_cost_per_month, None);
        assert_eq!(
            report.catalogs[0].schemas[0].tables[0].storage_cost_per_month,
            None
        );
    }

    #[test]
    fn a_bare_file_takes_the_local_table() {
        let report = fold(&[row(
            "catalogs/local/schemas/local/tables/quickstart.parquet",
            "text",
            8,
            10,
        )]);

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
        let directory = tempfile::tempdir().unwrap();
        let prefix = directory.path().join("masses");
        write_report(
            &prefix,
            &[row(
                "catalogs/local/schemas/local/tables/a<b>.parquet",
                "text",
                4,
                10,
            )],
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
    fn empty_rows_fail() {
        let directory = tempfile::tempdir().unwrap();
        let error = write_report(&directory.path().join("empty"), &[])
            .unwrap_err()
            .to_string();
        assert!(error.contains("summarized columns"), "{error}");
    }
}
