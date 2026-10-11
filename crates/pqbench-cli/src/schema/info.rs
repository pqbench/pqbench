//! `schema info`: one schema's record.

use futures_util::stream::StreamExt;
use pqbench::schema::info;

use super::{schema_ref, split_schema, NameArgs};
use crate::emit::Emitter;
use crate::source::{self, read_input};
use crate::CliError;

pub(super) async fn run(workspace: &pqbench::Workspace, args: &NameArgs) -> Result<(), CliError> {
    let input = read_input("schema info").await?;
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    if let Some(schema) = &args.schema {
        if input.first.is_some() {
            return Err("schema info takes CATALOG.SCHEMA or a schema stream, not both".into());
        }
        let (catalog, name) = split_schema("schema info", schema)?;
        let record = info::read(workspace, &catalog, &name).await?;
        emit.write_row(&record).await?;
        return emit.finish("schemas: 1\n").await;
    }
    if !input.piped {
        return Err("schema info needs CATALOG.SCHEMA or a schema stream".into());
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
        emit.write_row(&record?).await?;
        schemas += 1;
    }
    emit.finish(&format!("schemas: {schemas}\n")).await
}
