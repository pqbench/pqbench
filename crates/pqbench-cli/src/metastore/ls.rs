//! `metastore ls`: the catalogs at the endpoint.

use pqbench::metastore::ls;
use serde::Serialize;

use super::OutputArgs;
use crate::emit::{Align, Emitter, Row};
use crate::CliError;

/// The document `metastore ls` writes, one line per catalog.
#[derive(Serialize)]
struct CatalogRecord<'a> {
    kind: &'static str,
    version: u32,
    name: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    catalog_type: Option<&'a str>,
}

impl Row for CatalogRecord<'_> {
    const HEADER: &'static [&'static str] = &["name", "catalog_type"];
    const ALIGN: &'static [Align] = &[Align::Left; 2];

    fn cells(&self) -> Vec<String> {
        vec![
            self.name.to_string(),
            self.catalog_type.unwrap_or_default().to_string(),
        ]
    }
}

pub(super) async fn run_ls(
    workspace: &pqbench::Workspace,
    args: &OutputArgs,
) -> Result<(), CliError> {
    let catalogs = ls::list(workspace).await?;
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    for catalog in &catalogs {
        emit.write_row(&CatalogRecord {
            kind: "pqbench.catalog",
            version: 1,
            name: &catalog.name,
            catalog_type: catalog.catalog_type.as_deref(),
        })
        .await?;
    }
    emit.finish(&format!("catalogs: {}\n", catalogs.len()))
        .await
}
