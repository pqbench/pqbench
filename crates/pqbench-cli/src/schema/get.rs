//! `get schema`: one schema's record.

use futures_util::stream::StreamExt;
use pqbench::schema::get;

use super::NameArgs;
use crate::emit::Emitter;
use crate::error::{fail_when_dropped, ReadOutcome, RecordError};
use crate::source::{self, read_input, schema_ref, split_schema, Input};
use crate::CliError;

pub(crate) async fn run(workspace: &pqbench::Workspace, args: &NameArgs) -> Result<(), CliError> {
    let input = read_input("get schema").await?;
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    let (schemas, dropped) = match &args.schema {
        Some(schema) => {
            if input.first.is_some() {
                return Err("get schema takes CATALOG.SCHEMA or a schema stream, not both".into());
            }
            (get_one(workspace, schema, &mut emit).await?, 0)
        }
        None => get_stream(workspace, input, args.fan_out, &mut emit).await?,
    };
    emit.finish(&format!("schemas: {schemas}\n")).await?;
    fail_when_dropped(&[("schema", dropped)])
}

/// Read one named schema's record.
async fn get_one(
    workspace: &pqbench::Workspace,
    schema: &str,
    emit: &mut Emitter,
) -> Result<usize, CliError> {
    let (catalog, name) = split_schema("get schema", schema)?;
    let record = get::get(workspace, &pqbench::v1::name::schema(&catalog, &name)).await?;
    emit.write_row(&record).await?;
    Ok(1)
}

/// Read each schema's record from a schema stream.
async fn get_stream(
    workspace: &pqbench::Workspace,
    input: Input,
    fan_out: usize,
    emit: &mut Emitter,
) -> Result<(usize, usize), CliError> {
    if !input.piped {
        return Err("get schema needs CATALOG.SCHEMA or a schema stream".into());
    }
    let records = source::records("get schema", input.first, input.lines);
    let mut schemas = 0;
    let mut dropped = 0;
    let mut reads = records
        .map(|record| async {
            let name = schema_ref("get schema", &record?)?;
            let outcome = match get::get(workspace, &name).await {
                Ok(record) => ReadOutcome::Record(record),
                Err(error) => ReadOutcome::Dropped(RecordError::new("schema", &name, error)),
            };
            Ok::<_, CliError>(outcome)
        })
        .buffer_unordered(fan_out.max(1));
    while let Some(outcome) = reads.next().await {
        match outcome? {
            ReadOutcome::Record(record) => {
                emit.write_row(&record).await?;
                schemas += 1;
            }
            ReadOutcome::Dropped(error) => {
                error.report();
                dropped += 1;
            }
        }
    }
    Ok((schemas, dropped))
}
