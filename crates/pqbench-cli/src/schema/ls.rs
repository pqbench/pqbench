//! `schema ls`: the tables in a schema, one `Table` line each.

use futures_util::stream::StreamExt;
use pqbench::schema::ls;

use super::{schema_ref, split_schema, NameArgs};
use crate::emit::Emitter;
use crate::source::{self, read_input};
use crate::CliError;

pub(super) async fn run(workspace: &pqbench::Workspace, args: &NameArgs) -> Result<(), CliError> {
    let input = read_input("schema ls").await?;
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    if let Some(schema) = &args.schema {
        if input.first.is_some() {
            return Err("schema ls takes CATALOG.SCHEMA or a schema stream, not both".into());
        }
        let (catalog, name) = split_schema("schema ls", schema)?;
        let tables = ls::list(workspace, &catalog, &name).await?;
        for table in &tables {
            emit.write_row(table).await?;
        }
        return emit.finish(&format!("tables: {}\n", tables.len())).await;
    }
    if !input.piped {
        return Err("schema ls needs CATALOG.SCHEMA or a schema stream".into());
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
            emit.write_row(&table).await?;
            tables += 1;
        }
    }
    emit.finish(&format!("tables: {tables}\n")).await
}
