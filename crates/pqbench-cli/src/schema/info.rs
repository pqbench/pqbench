//! `schema info`: one schema's record.

use std::collections::BTreeMap;

use futures_util::stream::StreamExt;
use pqbench::schema::info;
use serde::Serialize;

use super::{schema_ref, split_schema, NameArgs};
use crate::emit::{Align, Emitter, Row};
use crate::source::{self, read_input};
use crate::CliError;

/// The document `schema info` writes.
#[derive(Serialize)]
struct SchemaRecord<'a> {
    kind: &'static str,
    version: u32,
    catalog: &'a str,
    name: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    comment: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    location: Option<&'a str>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    properties: &'a BTreeMap<String, String>,
}

impl Row for SchemaRecord<'_> {
    const HEADER: &'static [&'static str] = &["catalog", "name", "comment", "location"];
    const ALIGN: &'static [Align] = &[Align::Left; 4];

    fn cells(&self) -> Vec<String> {
        vec![
            self.catalog.to_string(),
            self.name.to_string(),
            self.comment.unwrap_or_default().to_string(),
            self.location.unwrap_or_default().to_string(),
        ]
    }
}

/// The row `schema info` writes for a schema.
fn schema_record(schema: &info::Schema) -> SchemaRecord<'_> {
    SchemaRecord {
        kind: "pqbench.schema",
        version: 1,
        catalog: &schema.catalog,
        name: &schema.name,
        comment: schema.comment.as_deref(),
        location: schema.location.as_deref(),
        properties: &schema.properties,
    }
}

pub(super) async fn run(workspace: &pqbench::Workspace, args: &NameArgs) -> Result<(), CliError> {
    let input = read_input("schema info").await?;
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    if let Some(schema) = &args.schema {
        if input.first.is_some() {
            return Err(
                "schema info takes CATALOG.SCHEMA or a pqbench.schema stream, not both".into(),
            );
        }
        let (catalog, name) = split_schema("schema info", schema)?;
        let record = info::read(workspace, &catalog, &name).await?;
        emit.write_row(&schema_record(&record)).await?;
        return emit.finish("schemas: 1\n").await;
    }
    if !input.piped {
        return Err("schema info needs CATALOG.SCHEMA or a pqbench.schema stream".into());
    }
    let records = source::records("schema info", input.first, input.lines);
    let mut schemas = 0;
    let mut reads = records
        .map(|record| async {
            let (catalog, name) = schema_ref(&record?)?;
            Ok::<_, CliError>(info::read(workspace, &catalog, &name).await?)
        })
        .buffer_unordered(args.fan_out.max(1));
    while let Some(record) = reads.next().await {
        emit.write_row(&schema_record(&record?)).await?;
        schemas += 1;
    }
    emit.finish(&format!("schemas: {schemas}\n")).await
}
