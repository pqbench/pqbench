//! `schema ls`: the tables in a schema, one `pqbench.table-ref` line each.

use futures_util::stream::StreamExt;
use pqbench::schema::ls;
use serde::Serialize;

use super::{schema_ref, split_schema, NameArgs};
use crate::emit::{Align, Emitter, Row};
use crate::source::{self, read_input};
use crate::CliError;

/// The document `schema ls` writes, one line per table. Version 2 is the
/// walk's ref: a ref is durable data — no env — so a later stage
/// (`table info`, `credentials get`) gets the lake source's options on its
/// own stdin.
#[derive(Serialize)]
struct TableRefRecord<'a> {
    kind: &'static str,
    version: u32,
    id: &'a str,
    uri: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    storage_path: Option<&'a str>,
}

impl Row for TableRefRecord<'_> {
    const HEADER: &'static [&'static str] = &["name", "uri", "storage path"];
    const ALIGN: &'static [Align] = &[Align::Left; 3];

    fn cells(&self) -> Vec<String> {
        vec![
            self.id.to_string(),
            self.uri.to_string(),
            self.storage_path.unwrap_or_default().to_string(),
        ]
    }
}

/// The row `schema ls` writes for a table.
fn table_record(table: &ls::TableRef) -> TableRefRecord<'_> {
    TableRefRecord {
        kind: "pqbench.table-ref",
        version: 2,
        id: &table.name,
        uri: &table.uri,
        storage_path: table.storage_path.as_deref(),
    }
}

pub(super) async fn run_ls(
    workspace: &pqbench::Workspace,
    args: &NameArgs,
) -> Result<(), CliError> {
    let input = read_input("schema ls").await?;
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    if let Some(schema) = &args.schema {
        if input.first.is_some() {
            return Err(
                "schema ls takes CATALOG.SCHEMA or a pqbench.schema stream, not both".into(),
            );
        }
        let (catalog, name) = split_schema("schema ls", schema)?;
        let tables = ls::list(workspace, &catalog, &name).await?;
        for table in &tables {
            emit.write_row(&table_record(table)).await?;
        }
        return emit.finish(&format!("tables: {}\n", tables.len())).await;
    }
    if !input.piped {
        return Err("schema ls needs CATALOG.SCHEMA or a pqbench.schema stream".into());
    }
    let records = source::records("schema ls", input.first, input.lines);
    let mut tables = 0;
    let mut lists = records
        .map(|record| async {
            let (catalog, name) = schema_ref(&record?)?;
            Ok::<_, CliError>(ls::list(workspace, &catalog, &name).await?)
        })
        .buffer_unordered(args.fan_out.max(1));
    while let Some(listed) = lists.next().await {
        for table in listed? {
            emit.write_row(&table_record(&table)).await?;
            tables += 1;
        }
    }
    emit.finish(&format!("tables: {tables}\n")).await
}
