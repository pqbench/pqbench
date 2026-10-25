//! `credentials get`: put vended read credentials on each table-ref:
//! `list tables | credentials get`.
//!
//! The verb is `name -> lease`; the stage is what writes the lease onto the ref
//! it read, so the pipe carries on with the same record, enriched.

use futures_util::stream::StreamExt;
use serde::Serialize;
use serde_json::Value;

use super::StageArgs;
use crate::document::invalid_json;
use crate::emit::{Align, Emitter, Row};
use crate::error::{fail_when_dropped, ReadOutcome, RecordError};
use crate::source::{self, dotted_name, read_input};
use crate::CliError;

pub(super) async fn run(workspace: &pqbench::Workspace, args: &StageArgs) -> Result<(), CliError> {
    let input = read_input("credentials get").await?;
    if !input.piped {
        return Err("credentials get reads table refs on standard input".into());
    }
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    let records = source::records("credentials get", input.first, input.lines);
    let mut tables = 0;
    let mut dropped = 0;
    let mut vends = records
        .map(|record| async {
            let table: pqbench::v1::Table =
                serde_json::from_value(record?).map_err(invalid_json)?;
            let outcome = match pqbench::credentials::get::get(workspace, &table.name).await {
                Ok(credentials) => ReadOutcome::Record((table, credentials)),
                Err(error) => {
                    ReadOutcome::Dropped(RecordError::new("table", dotted_name(&table.name), error))
                }
            };
            Ok::<_, CliError>(outcome)
        })
        .buffer_unordered(args.fan_out.max(1));
    while let Some(outcome) = vends.next().await {
        let (mut table, credentials) = match outcome? {
            ReadOutcome::Record((table, credentials)) => (table, credentials),
            ReadOutcome::Dropped(error) => {
                error.report();
                dropped += 1;
                continue;
            }
        };
        if let Some(credentials) = credentials {
            table.credentials = Some(credentials);
        }
        emit.write_row(&VendedRef {
            vended: table.credentials.is_some(),
            record: serde_json::to_value(&table).unwrap_or(Value::Null),
        })
        .await?;
        tables += 1;
    }
    emit.finish(&format!("tables: {tables}\n")).await?;
    fail_when_dropped(&[("table", dropped)])
}

/// The row `credentials get` writes: the ref, plus whether creds were vended.
/// The serialized form is the ref itself, with its lease on it.
struct VendedRef {
    record: Value,
    vended: bool,
}

impl Serialize for VendedRef {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.record.serialize(serializer)
    }
}

impl Row for VendedRef {
    const HEADER: &'static [&'static str] = &["name", "credentials"];
    const ALIGN: &[Align] = &[Align::Left, Align::Left];

    fn cells(&self) -> Vec<String> {
        let name = self.record["name"]
            .as_str()
            .or_else(|| self.record["id"].as_str())
            .unwrap_or_default();
        vec![
            name.to_string(),
            if self.vended { "vended" } else { "-" }.to_string(),
        ]
    }
}
