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

/// One catalog: the total over its schemas.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct CatalogNode {
    /// `catalogs/{catalog}`.
    pub name: String,
    /// The user-facing label: the catalog id.
    pub display_name: String,
    /// On-disk bytes over every measured table.
    pub compressed_bytes: u64,
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
}

/// Write `path.html` from the measured columns.
///
/// # Errors
/// Fails when there are no columns or the HTML page cannot be written.
pub fn write_report(prefix: &Path, columns: &[v1::Bytemass]) -> Result<(), Error> {
    let report = fold(columns);
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

/// What one table's columns add up to while the report is built.
#[derive(Default)]
struct TableBytes {
    compressed_bytes: u64,
    columns: Vec<ColumnNode>,
    blobs: BTreeSet<String>,
}

/// Group the columns by their table and fold the totals up the hierarchy.
fn fold(columns: &[v1::Bytemass]) -> Report {
    let mut tables: BTreeMap<(String, String, String), TableBytes> = BTreeMap::new();
    for column in columns {
        let Some((blob, path)) = v1::name::parse_bytemass(&column.name) else {
            continue;
        };
        let Some(table) = resolve_table(column, &blob) else {
            continue;
        };
        let Some((catalog, schema, name)) = v1::Table::parse_name(&table) else {
            continue;
        };
        let bytes = column.compressed_bytes.unwrap_or(0);
        let values = column.value_count.unwrap_or(0);
        let table = tables.entry((catalog, schema, name)).or_default();
        table.compressed_bytes += bytes;
        table.blobs.insert(blob);
        table.columns.push(ColumnNode {
            column: path,
            compressed_bytes: bytes,
            value_count: values,
            bytes_per_value: if values == 0 {
                0.0
            } else {
                bytes as f64 / values as f64
            },
        });
    }

    let mut report = Report::default();
    for ((catalog, schema, name), table) in tables {
        let bytes = table.compressed_bytes;
        let node = TableNode {
            name: v1::name::table(&catalog, &schema, &name),
            display_name: name,
            compressed_bytes: bytes,
            columns: table.columns,
            file_count: table.blobs.len() as u64,
        };
        {
            let catalog_node = ensure_catalog(&mut report, &catalog);
            let schema_node = ensure_schema(catalog_node, &schema);
            schema_node.compressed_bytes += bytes;
            schema_node.tables.push(node);
        }
        ensure_catalog(&mut report, &catalog).compressed_bytes += bytes;
    }
    report
}

/// The table a measurement names: the table-file's table, or the local table a
/// bare path takes (the data model gives a path-read table `local.local`).
fn resolve_table(column: &v1::Bytemass, blob: &str) -> Option<String> {
    column.table.clone().or_else(|| {
        let uri = v1::name::blob_uri(blob)?;
        Some(v1::name::local_table(&uri))
    })
}

/// The catalog node, appending it when the fold reaches a new catalog.
fn ensure_catalog<'a>(report: &'a mut Report, catalog: &str) -> &'a mut CatalogNode {
    let name = v1::name::catalog(catalog);
    if report.catalogs.last().map(|node| node.name.as_str()) != Some(name.as_str()) {
        report.catalogs.push(CatalogNode {
            name,
            display_name: catalog.to_string(),
            ..CatalogNode::default()
        });
    }
    report
        .catalogs
        .last_mut()
        .expect("a catalog was just pushed")
}

/// The schema node, appending it when the fold reaches a new schema.
fn ensure_schema<'a>(catalog: &'a mut CatalogNode, schema: &str) -> &'a mut SchemaNode {
    let name = v1::name::schema(&catalog.display_name, schema);
    if catalog.schemas.last().map(|node| node.name.as_str()) != Some(name.as_str()) {
        catalog.schemas.push(SchemaNode {
            name,
            display_name: schema.to_string(),
            ..SchemaNode::default()
        });
    }
    catalog
        .schemas
        .last_mut()
        .expect("a schema was just pushed")
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

    fn column(file: &v1::Blob, table: Option<&str>, column: &str, bytes: u64) -> v1::Bytemass {
        v1::Bytemass {
            name: name::bytemass(&file.name, column),
            table: table.map(str::to_string),
            compressed_bytes: Some(bytes),
            value_count: Some(10),
            ..v1::Bytemass::default()
        }
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
        let report = fold(&columns);

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
    fn a_bare_file_takes_the_local_table() {
        let file = blob("/lake/quickstart.parquet");
        let report = fold(&[column(&file, None, "text", 8)]);

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
        write_report(&prefix, &[column(&file, None, "text", 4)]).unwrap();

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
        let error = write_report(&directory.path().join("empty"), &[])
            .unwrap_err()
            .to_string();
        assert!(error.contains("bytemass columns"), "{error}");
    }
}
