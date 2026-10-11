//! `catalog ls`: the schemas in a catalog, one `pqbench.schema` line each.

use futures_util::stream::StreamExt;
use pqbench::catalog::ls;
use serde::Serialize;

use super::{catalog_name, NameArgs};
use crate::emit::{Align, Emitter, Row};
use crate::source::{self, read_input};
use crate::CliError;

/// The document `catalog ls` writes, one line per schema.
#[derive(Serialize)]
struct SchemaRecord<'a> {
    kind: &'static str,
    version: u32,
    catalog: &'a str,
    name: &'a str,
}

impl Row for SchemaRecord<'_> {
    const HEADER: &'static [&'static str] = &["catalog", "name"];
    const ALIGN: &'static [Align] = &[Align::Left; 2];

    fn cells(&self) -> Vec<String> {
        vec![self.catalog.to_string(), self.name.to_string()]
    }
}

/// The row `catalog ls` writes for a schema.
fn schema_record(schema: &ls::Schema) -> SchemaRecord<'_> {
    SchemaRecord {
        kind: "pqbench.schema",
        version: 1,
        catalog: &schema.catalog,
        name: &schema.name,
    }
}

pub(super) async fn run(workspace: &pqbench::Workspace, args: &NameArgs) -> Result<(), CliError> {
    let input = read_input("catalog ls").await?;
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    if let Some(name) = &args.catalog {
        if input.first.is_some() {
            return Err("catalog ls takes CATALOG or a pqbench.catalog stream, not both".into());
        }
        let schemas = ls::list(workspace, name).await?;
        for schema in &schemas {
            emit.write_row(&schema_record(schema)).await?;
        }
        return emit.finish(&format!("schemas: {}\n", schemas.len())).await;
    }
    if !input.piped {
        return Err("catalog ls needs CATALOG or a pqbench.catalog stream".into());
    }
    let records = source::records("catalog ls", input.first, input.lines);
    let mut schemas = 0;
    let mut lists = records
        .map(|record| async {
            let name = catalog_name(&record?)?;
            Ok::<_, CliError>(ls::list(workspace, &name).await?)
        })
        .buffer_unordered(args.fan_out.max(1));
    while let Some(listed) = lists.next().await {
        for schema in listed? {
            emit.write_row(&schema_record(&schema)).await?;
            schemas += 1;
        }
    }
    emit.finish(&format!("schemas: {schemas}\n")).await
}
