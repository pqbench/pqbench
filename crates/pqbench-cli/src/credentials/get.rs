//! `credentials get`: put vended read credentials on each table-ref:
//! `list tables | credentials get`.
//!
//! The verb is `name -> lease`; the stage is what writes the lease onto the ref
//! it read, so the pipe carries on with the same record, enriched.

use super::StageArgs;
use crate::document::invalid_json;
use crate::emit::Emitter;
use crate::error::{context, fail_when_dropped};
use crate::source::{self, dotted_name, read_input};
use crate::CliError;

pub(super) async fn run(workspace: &pqbench::Workspace, args: &StageArgs) -> Result<(), CliError> {
    let input = read_input("credentials get").await?;
    if !input.piped {
        return Err("credentials get reads table refs on standard input".into());
    }
    let mut emit = Emitter::open();
    let records = source::records("credentials get", input.first, input.lines);
    let dropped = source::stage(
        records,
        args.fan_out,
        &mut emit,
        |record: serde_json::Value| async move {
            let mut table: pqbench::v1::Table =
                serde_json::from_value(record).map_err(invalid_json)?;
            let credentials = pqbench::credentials::get::get(workspace, &table.name)
                .await
                .map_err(|cause| context(dotted_name(&table.name), cause))?;
            if let Some(credentials) = credentials {
                table.credentials = Some(credentials);
            }
            Ok(vec![table])
        },
    )
    .await?;
    emit.finish().await?;
    fail_when_dropped(dropped)
}
