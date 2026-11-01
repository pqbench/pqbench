//! Table rendering for records whose columns are curated.
//!
//! The CLI forwards the core objects as-is, so a type's columns live here (the
//! trait is the CLI's, the types the library's) rather than in a per-command
//! wrapper struct. `pqbench format` is the only caller.

use pqbench::mass;
use pqbench::v1;
use serde_json::Value;

use crate::CliError;

/// How a table column pads its cells.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Align {
    Left,
    Right,
}

/// A record that renders as one table row.
pub(crate) trait Row {
    /// Column headers.
    const HEADER: &'static [&'static str];
    /// Per-column alignment, the same length as `HEADER`.
    const ALIGN: &'static [Align];
    /// One cell per column.
    fn cells(&self) -> Vec<String>;
}

impl Row for v1::Metastore {
    const HEADER: &'static [&'static str] = &["name", "uid", "cloud_provider", "region"];
    const ALIGN: &[Align] = &[Align::Left; 4];

    fn cells(&self) -> Vec<String> {
        vec![
            self.name.clone(),
            self.uid.clone().unwrap_or_default(),
            self.cloud_provider.clone().unwrap_or_default(),
            self.region.clone().unwrap_or_default(),
        ]
    }
}

impl Row for v1::Catalog {
    const HEADER: &'static [&'static str] = &["name", "catalog_type"];
    const ALIGN: &[Align] = &[Align::Left; 2];

    fn cells(&self) -> Vec<String> {
        vec![
            self.name.clone(),
            self.catalog_type
                .map(|kind| kind.as_str())
                .unwrap_or_default()
                .to_string(),
        ]
    }
}

impl Row for v1::Schema {
    const HEADER: &'static [&'static str] = &["name", "comment", "uri"];
    const ALIGN: &[Align] = &[Align::Left; 3];

    fn cells(&self) -> Vec<String> {
        vec![
            self.name.clone(),
            self.comment.clone().unwrap_or_default(),
            self.uri.clone().unwrap_or_default(),
        ]
    }
}

impl Row for v1::Table {
    const HEADER: &'static [&'static str] =
        &["name", "format", "snapshot_version", "columns", "uri"];
    const ALIGN: &[Align] = &[
        Align::Left,
        Align::Left,
        Align::Right,
        Align::Right,
        Align::Left,
    ];

    fn cells(&self) -> Vec<String> {
        vec![
            self.name.clone(),
            format_name(self.format).to_string(),
            self.snapshot_version
                .map(|version| version.to_string())
                .unwrap_or_default(),
            self.declared_columns.len().to_string(),
            self.uri.clone().unwrap_or_default(),
        ]
    }
}

/// The table format as the human table spells it.
fn format_name(format: Option<v1::TableFormat>) -> &'static str {
    match format {
        Some(v1::TableFormat::Delta) => "delta",
        Some(v1::TableFormat::Iceberg) => "iceberg",
        _ => "unspecified",
    }
}

impl Row for v1::TableFile {
    const HEADER: &'static [&'static str] = &["path", "size_bytes"];
    const ALIGN: &[Align] = &[Align::Left, Align::Right];

    fn cells(&self) -> Vec<String> {
        let path = v1::name::parse_table_file(&self.name)
            .map(|(_, path)| path)
            .unwrap_or_else(|| self.name.clone());
        vec![
            path,
            self.size_bytes
                .map(|size| size.to_string())
                .unwrap_or_default(),
        ]
    }
}

impl Row for v1::Column {
    const HEADER: &'static [&'static str] = &[
        "column",
        "type",
        "codec",
        "encoding",
        "bytes",
        "dict bytes",
        "bytes/row",
        "values",
    ];
    const ALIGN: &[Align] = &[
        Align::Left,
        Align::Left,
        Align::Left,
        Align::Left,
        Align::Right,
        Align::Right,
        Align::Right,
        Align::Right,
    ];

    fn cells(&self) -> Vec<String> {
        let name = v1::Column::parse_name(&self.name)
            .map(|(_, _, column)| column)
            .unwrap_or_else(|| self.name.clone());
        let mass = self.mass.as_ref();
        vec![
            name,
            self.physical_data_type.clone().unwrap_or_default(),
            mass.and_then(|mass| mass.codec.clone()).unwrap_or_default(),
            mass.map(|mass| encoding_text(mass, self.physical_data_type.as_deref()))
                .unwrap_or_default(),
            mass.and_then(|mass| mass.compressed_bytes)
                .unwrap_or(0)
                .to_string(),
            mass.map(|mass| {
                if mass.dictionary {
                    mass.dictionary_bytes
                        .map(|bytes| bytes.to_string())
                        .unwrap_or_default()
                } else {
                    "0".to_string()
                }
            })
            .unwrap_or_default(),
            mass.and_then(|mass| mass.bytes_per_row)
                .map(|bytes| format!("{bytes:.2}"))
                .unwrap_or_default(),
            mass.and_then(|mass| mass.value_count)
                .unwrap_or(0)
                .to_string(),
        ]
    }
}

impl Row for mass::ColumnMass {
    const HEADER: &'static [&'static str] = &[
        "table",
        "column",
        "bytes",
        "values",
        "bytes/value",
        "ratio",
        "files",
        "class",
        "$/month",
    ];
    const ALIGN: &[Align] = &[
        Align::Left,
        Align::Left,
        Align::Right,
        Align::Right,
        Align::Right,
        Align::Right,
        Align::Right,
        Align::Left,
        Align::Right,
    ];

    fn cells(&self) -> Vec<String> {
        let table = v1::Table::parse_name(&self.table)
            .map(|(_, schema, table)| format!("{schema}.{table}"))
            .unwrap_or_else(|| self.table.clone());
        vec![
            table,
            self.column.clone(),
            self.compressed_bytes.to_string(),
            self.value_count.to_string(),
            format!("{:.2}", self.bytes_per_value),
            self.compression_ratio
                .map(|ratio| format!("{ratio:.2}"))
                .unwrap_or_default(),
            self.file_count.to_string(),
            self.storage_classes.join(","),
            cost_text(self),
        ]
    }
}

/// A row's monthly cost, as the page shows it: classes add, sub-tiers range.
fn cost_text(row: &mass::ColumnMass) -> String {
    let Some((first, last)) = row.cost_range() else {
        return String::new();
    };
    let (first, last) = (dollars(first), dollars(last));
    if first == last {
        first
    } else {
        format!("{first}–{last}")
    }
}

/// A monthly cost in dollars, at the precision the page uses.
fn dollars(value: f64) -> String {
    if value >= 1000.0 {
        format!("${:.1}k", value / 1000.0)
    } else if value >= 1.0 {
        format!("${value:.2}")
    } else if value >= 0.01 {
        format!("${value:.3}")
    } else {
        format!("${value:.4}")
    }
}

/// The data-page encodings, with the dictionary spellings collapsed to `dict`.
///
/// The footer's `encodings` mixes the dictionary page and level encodings with
/// the data-page encodings; `data_encodings` is exact when the writer recorded
/// page statistics. Otherwise the renderer falls back to the raw set, dropping
/// the level-only `RLE` for non-boolean columns.
fn encoding_text(mass: &v1::Bytemass, physical_data_type: Option<&str>) -> String {
    if let Some(encodings) = &mass.data_encodings {
        return summarize_encodings(encodings.iter().map(String::as_str));
    }
    if mass.dictionary {
        return "dict".to_string();
    }
    let boolean = physical_data_type == Some("BOOLEAN");
    let data: Vec<&str> = mass
        .encodings
        .iter()
        .map(String::as_str)
        .filter(|encoding| boolean || *encoding != "RLE")
        .collect();
    if data.is_empty() {
        summarize_encodings(mass.encodings.iter().map(String::as_str))
    } else {
        summarize_encodings(data)
    }
}

/// Join encoding names, spelling both dictionary encodings as `dict`.
fn summarize_encodings<'a>(encodings: impl IntoIterator<Item = &'a str>) -> String {
    encodings
        .into_iter()
        .map(|encoding| match encoding {
            "RLE_DICTIONARY" | "PLAIN_DICTIONARY" => "dict",
            other => other,
        })
        .collect::<Vec<_>>()
        .join("+")
}

/// One table from records of a single curated type.
pub(crate) fn render<T: Row>(rows: &[T]) -> String {
    if rows.is_empty() {
        return String::new();
    }
    render_cells(
        T::HEADER,
        T::ALIGN,
        &rows.iter().map(Row::cells).collect::<Vec<_>>(),
    )
}

/// One table from records with no curated renderer: their keys, in order.
///
/// Numbers right-align; everything else left-aligns.
pub(crate) fn render_values(rows: &[Value]) -> String {
    let Some(first) = rows.first().and_then(Value::as_object) else {
        return String::new();
    };
    let header: Vec<&str> = first.keys().map(String::as_str).collect();
    let align: Vec<Align> = header
        .iter()
        .map(|key| {
            if rows
                .iter()
                .all(|row| row.get(key).is_none_or(Value::is_number))
            {
                Align::Right
            } else {
                Align::Left
            }
        })
        .collect();
    let cells: Vec<Vec<String>> = rows
        .iter()
        .map(|row| header.iter().map(|key| cell(row.get(*key))).collect())
        .collect();
    render_cells(&header, &align, &cells)
}

/// One JSON value as a table cell.
fn cell(value: Option<&Value>) -> String {
    match value {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(text)) => text.clone(),
        Some(other) => other.to_string(),
    }
}

fn render_cells(header: &[&str], align: &[Align], rows: &[Vec<String>]) -> String {
    let mut widths: Vec<usize> = header.iter().map(|text| text.chars().count()).collect();
    for row in rows {
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.chars().count());
        }
    }
    let mut out = String::new();
    out.push_str(&format_row(header.iter().copied(), &widths, align));
    let rule: Vec<String> = widths.iter().map(|width| "-".repeat(*width)).collect();
    out.push_str(&format_row(rule.iter().map(String::as_str), &widths, align));
    for row in rows {
        out.push_str(&format_row(row.iter().map(String::as_str), &widths, align));
    }
    out
}

fn format_row<'a>(
    cells: impl Iterator<Item = &'a str>,
    widths: &[usize],
    align: &[Align],
) -> String {
    let mut line = String::new();
    for (index, (cell, width)) in cells.zip(widths).enumerate() {
        if index > 0 {
            line.push_str("  ");
        }
        let pad = width.saturating_sub(cell.chars().count());
        match align.get(index).copied().unwrap_or(Align::Left) {
            Align::Right => {
                line.extend(std::iter::repeat_n(' ', pad));
                line.push_str(cell);
            }
            Align::Left => {
                line.push_str(cell);
                line.extend(std::iter::repeat_n(' ', pad));
            }
        }
    }
    while line.ends_with(' ') {
        line.pop();
    }
    line.push('\n');
    line
}

/// Write rendered text to standard output. A closed reader is not an error.
pub(crate) async fn write(text: &str) -> Result<(), CliError> {
    use tokio::io::AsyncWriteExt;

    let mut stdout = tokio::io::stdout();
    let written = match stdout.write_all(text.as_bytes()).await {
        Ok(()) => stdout.flush().await,
        Err(error) => Err(error),
    };
    match written {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::BrokenPipe => Ok(()),
        Err(error) => Err(error.into()),
    }
}

/// One codec sweep row (`lz`, `compression`).
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct SweepRow {
    pub name: String,
    #[serde(flatten)]
    pub row: pqbench::report::ReportRow,
}

impl Row for SweepRow {
    const HEADER: &'static [&'static str] = &[
        "codec",
        "level",
        "compress MB/s",
        "decompress MB/s",
        "ratio",
    ];
    const ALIGN: &[Align] = &[
        Align::Left,
        Align::Right,
        Align::Right,
        Align::Right,
        Align::Right,
    ];

    fn cells(&self) -> Vec<String> {
        let row = &self.row;
        let bytes = row.uncompressed_bytes as u64;
        vec![
            row.codec.to_string(),
            row.level.to_string(),
            format!("{:.1}", row.compress_estimate.megabytes_per_second(bytes)),
            format!("{:.1}", row.decompress_estimate.megabytes_per_second(bytes)),
            format!("{:.2}", row.ratio),
        ]
    }
}

/// One per-column sweep row (`compression --per-column`).
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct ColumnSweepRow {
    pub name: String,
    #[serde(flatten)]
    pub row: pqbench::report::ColumnRow,
}

impl Row for ColumnSweepRow {
    const HEADER: &'static [&'static str] = &[
        "codec",
        "level",
        "column",
        "compress MB/s",
        "decompress MB/s",
        "ratio",
    ];
    const ALIGN: &[Align] = &[
        Align::Left,
        Align::Right,
        Align::Left,
        Align::Right,
        Align::Right,
        Align::Right,
    ];

    fn cells(&self) -> Vec<String> {
        let row = &self.row;
        let bytes = row.uncompressed_bytes as u64;
        vec![
            row.codec.to_string(),
            row.level.to_string(),
            row.column.clone(),
            format!("{:.1}", row.compress_estimate.megabytes_per_second(bytes)),
            format!("{:.1}", row.decompress_estimate.megabytes_per_second(bytes)),
            format!("{:.2}", row.ratio),
        ]
    }
}

/// One column-profile row (`profile`).
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct ProfileRow {
    pub name: String,
    pub id: String,
    #[serde(flatten)]
    pub profile: pqbench::profile::ColumnProfile,
}

impl Row for ProfileRow {
    const HEADER: &'static [&'static str] = &["id", "column", "kind", "values", "nulls", "ndv"];
    const ALIGN: &[Align] = &[
        Align::Left,
        Align::Left,
        Align::Left,
        Align::Right,
        Align::Right,
        Align::Right,
    ];

    fn cells(&self) -> Vec<String> {
        vec![
            self.id.clone(),
            self.profile.column.clone(),
            self.profile.physical_kind.clone(),
            self.profile.num_values.to_string(),
            self.profile.null_count.to_string(),
            self.profile.ndv.to_string(),
        ]
    }
}
