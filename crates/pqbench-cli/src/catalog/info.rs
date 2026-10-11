//! `catalog info`: one catalog's record.

use futures_util::stream::StreamExt;
use pqbench::catalog::info;
use serde::Serialize;

use super::{catalog_name, NameArgs};
use crate::emit::{Align, Emitter, Row};
use crate::source::{self, read_input};
use crate::CliError;

/// The document `catalog info` writes.
#[derive(Serialize)]
struct CatalogRecord<'a> {
    kind: &'static str,
    version: u32,
    name: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    catalog_type: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    comment: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    owner: Option<&'a str>,
}

impl Row for CatalogRecord<'_> {
    const HEADER: &'static [&'static str] = &["name", "catalog_type", "comment", "owner"];
    const ALIGN: &'static [Align] = &[Align::Left; 4];

    fn cells(&self) -> Vec<String> {
        vec![
            self.name.to_string(),
            self.catalog_type.unwrap_or_default().to_string(),
            self.comment.unwrap_or_default().to_string(),
            self.owner.unwrap_or_default().to_string(),
        ]
    }
}

/// The row `catalog info` writes for a catalog.
fn catalog_record(catalog: &info::Catalog) -> CatalogRecord<'_> {
    CatalogRecord {
        kind: "pqbench.catalog",
        version: 1,
        name: &catalog.name,
        catalog_type: catalog.catalog_type.as_deref(),
        comment: catalog.comment.as_deref(),
        owner: catalog.owner.as_deref(),
    }
}

pub(super) async fn run(workspace: &pqbench::Workspace, args: &NameArgs) -> Result<(), CliError> {
    let input = read_input("catalog info").await?;
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    if let Some(name) = &args.catalog {
        if input.first.is_some() {
            return Err("catalog info takes CATALOG or a pqbench.catalog stream, not both".into());
        }
        let catalog = info::read(workspace, name).await?;
        emit.write_row(&catalog_record(&catalog)).await?;
        return emit.finish("catalogs: 1\n").await;
    }
    if !input.piped {
        return Err("catalog info needs CATALOG or a pqbench.catalog stream".into());
    }
    let records = source::records("catalog info", input.first, input.lines);
    let mut catalogs = 0;
    let mut reads = records
        .map(|record| async {
            let name = catalog_name(&record?)?;
            Ok::<_, CliError>(info::read(workspace, &name).await?)
        })
        .buffer_unordered(args.fan_out.max(1));
    while let Some(catalog) = reads.next().await {
        emit.write_row(&catalog_record(&catalog?)).await?;
        catalogs += 1;
    }
    emit.finish(&format!("catalogs: {catalogs}\n")).await
}
