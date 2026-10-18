//! `get schema`: one schema's record.

use futures_util::stream::StreamExt;
use pqbench::schema::get;

use super::NameArgs;
use crate::emit::Emitter;
use crate::source::{self, read_input, schema_ref, split_schema};
use crate::CliError;

pub(crate) async fn run(workspace: &pqbench::Workspace, args: &NameArgs) -> Result<(), CliError> {
    let input = read_input("get schema").await?;
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    if let Some(schema) = &args.schema {
        if input.first.is_some() {
            return Err("get schema takes CATALOG.SCHEMA or a schema stream, not both".into());
        }
        let (catalog, name) = split_schema("get schema", schema)?;
        let record = get::get(workspace, &pqbench::v1::name::schema(&catalog, &name)).await?;
        emit.write_row(&record).await?;
        return emit.finish("schemas: 1\n").await;
    }
    if !input.piped {
        return Err("get schema needs CATALOG.SCHEMA or a schema stream".into());
    }
    let records = source::records("get schema", input.first, input.lines);
    let mut schemas = 0;
    let mut reads = records
        .map(|record| async {
            let name = schema_ref("get schema", &record?)?;
            Ok::<_, CliError>(get::get(workspace, &name).await?)
        })
        .buffer_unordered(args.fan_out.max(1));
    while let Some(record) = reads.next().await {
        emit.write_row(&record?).await?;
        schemas += 1;
    }
    emit.finish(&format!("schemas: {schemas}\n")).await
}
