//! `credentials check`: filter each ref's table on the catalog's eligibility:
//! `list tables | credentials check | credentials get`.
//!
//! The check is the walk's single filter. It drops a `system` catalog ref, a
//! view (the catalog reports no location), and a table whose capability
//! manifest marks compute-only (managed default storage), writing the reason to
//! standard error. Eligible refs pass through unchanged, so the stage composes
//! ahead of `credentials get` and a mixed schema keeps going. A drop is the
//! filter working, not a failure: a whole-catalog walk always has tables it
//! cannot read outside compute, so the stage still succeeds and only a real
//! error (a missing table, an endpoint fault) fails it.

use futures_util::stream::StreamExt;
use pqbench::credentials::check::{Eligibility, Reason};
use serde::Serialize;
use serde_json::Value;

use super::StageArgs;
use crate::emit::{Align, Emitter, Row};
use crate::source::{self, read_input, table_ref};
use crate::CliError;

pub(super) async fn run(workspace: &pqbench::Workspace, args: &StageArgs) -> Result<(), CliError> {
    let input = read_input("credentials check").await?;
    if !input.piped {
        return Err("credentials check reads table refs on standard input".into());
    }
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    let records = source::records("credentials check", input.first, input.lines);
    let mut tables = 0;
    let mut dropped = 0;
    let mut checks = records
        .map(|record| async {
            let record = record?;
            let (catalog, schema, name) = table_ref("credentials check", &record)?;
            let reason = check(workspace, &catalog, &schema, &name).await?;
            Ok::<_, CliError>((record, reason))
        })
        .buffer_unordered(args.fan_out.max(1));
    while let Some(checked) = checks.next().await {
        let (record, reason) = checked?;
        if let Some(reason) = reason {
            eprintln!("error: {reason}");
            dropped += 1;
            continue;
        }
        emit.write_row(&CheckedRef { record }).await?;
        tables += 1;
    }
    emit.finish(&format!("tables: {tables}\n")).await?;
    // A drop is the filter working, not a failure: the walk composes on this
    // stage for a mixed schema, and a whole-catalog walk always has tables
    // (system, views, managed default storage) it cannot read outside compute.
    // Only a real error — a missing table, an endpoint fault — fails the stage.
    if dropped > 0 {
        eprintln!("{dropped} table(s) are not readable outside Databricks compute");
    }
    Ok(())
}

/// The reason a table is not readable outside Databricks compute, `None` when
/// it is. The core check owns the dialect — Unity reads the capability
/// manifest; Iceberg REST reads its metadata inline through the catalog, so it
/// passes — and the CLI only renders the reason.
async fn check(
    workspace: &pqbench::Workspace,
    catalog: &str,
    schema: &str,
    name: &str,
) -> Result<Option<String>, CliError> {
    let eligibility = pqbench::credentials::check::check(workspace, catalog, schema, name).await?;
    match eligibility {
        Eligibility::Eligible => Ok(None),
        Eligibility::Ineligible(reason) => Ok(Some(ineligible(
            &format!("{catalog}.{schema}.{name}"),
            reason,
        ))),
    }
}

/// The message an ineligible table reports.
fn ineligible(name: &str, reason: Reason) -> String {
    let because = match reason {
        Reason::SystemTable => "a system table",
        Reason::NotATable => "a view, not a table",
        Reason::NoExternalRead => "a table only Databricks compute reads (managed default storage)",
    };
    format!(
        "{name} is {because}: read it with Databricks compute, or copy it to an external location"
    )
}

/// The row `credentials check` writes: the ref, gated on eligibility. The
/// serialized form is the ref itself.
struct CheckedRef {
    record: Value,
}

impl Serialize for CheckedRef {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.record.serialize(serializer)
    }
}

impl Row for CheckedRef {
    const HEADER: &'static [&'static str] = &["name", "eligible"];
    const ALIGN: &[Align] = &[Align::Left, Align::Left];

    fn cells(&self) -> Vec<String> {
        vec![
            self.record["id"].as_str().unwrap_or_default().to_string(),
            "yes".to_string(),
        ]
    }
}
