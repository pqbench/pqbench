//! `schema get`: one schema's record.

use futures_util::stream::StreamExt;
use pqbench::schema::get;

use super::NameArgs;
use crate::emit::Emitter;
use crate::source::{self, read_input, schema_ref, split_schema};
use crate::CliError;

pub(crate) async fn run(workspace: &pqbench::Workspace, args: &NameArgs) -> Result<(), CliError> {
    let input = read_input("schema get").await?;
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    if let Some(schema) = &args.schema {
        if input.first.is_some() {
            return Err("schema get takes CATALOG.SCHEMA or a schema stream, not both".into());
        }
        let (catalog, name) = split_schema("schema get", schema)?;
        let record = get::read(workspace, &catalog, &name).await?;
        emit.write_row(&record).await?;
        return emit.finish("schemas: 1\n").await;
    }
    if !input.piped {
        return Err("schema get needs CATALOG.SCHEMA or a schema stream".into());
    }
    let records = source::records("schema get", input.first, input.lines);
    let mut schemas = 0;
    let mut reads = records
        .map(|record| async {
            let (catalog, name) = schema_ref("schema get", &record?)?;
            Ok::<_, CliError>(get::read(workspace, &catalog, &name).await?)
        })
        .buffer_unordered(args.fan_out.max(1));
    while let Some(record) = reads.next().await {
        emit.write_row(&record?).await?;
        schemas += 1;
    }
    emit.finish(&format!("schemas: {schemas}\n")).await
}
