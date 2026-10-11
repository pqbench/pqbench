//! Table rendering for the core wire types.
//!
//! The CLI forwards the core objects as-is, so a type's columns live here
//! (the trait is the CLI's, the types the library's) rather than in a
//! per-command wrapper struct. A stream is one type, so one impl per type.

use pqbench::v1;

use crate::emit::{Align, Row};

impl Row for v1::Metastore {
    const HEADER: &'static [&'static str] = &["name", "uid", "cloud_provider", "region"];
    const ALIGN: &'static [Align] = &[Align::Left; 4];

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
    const ALIGN: &'static [Align] = &[Align::Left; 2];

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
    const ALIGN: &'static [Align] = &[Align::Left; 3];

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
    const ALIGN: &'static [Align] = &[
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

impl Row for v1::Partition {
    const HEADER: &'static [&'static str] = &["name", "first_time", "last_time", "commits"];
    const ALIGN: &'static [Align] = &[Align::Left, Align::Right, Align::Right, Align::Right];

    fn cells(&self) -> Vec<String> {
        vec![
            self.name.clone(),
            self.start_time
                .map(|time| time.to_string())
                .unwrap_or_default(),
            self.end_time
                .map(|time| time.to_string())
                .unwrap_or_default(),
            self.commit_versions.len().to_string(),
        ]
    }
}

impl Row for v1::TableFile {
    const HEADER: &'static [&'static str] = &["path", "size_bytes"];
    const ALIGN: &'static [Align] = &[Align::Left, Align::Right];

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
