//! `credentials check`: filter each ref's table on the catalog's eligibility:
//! `list tables | credentials check | credentials get`.
//!
//! The check is the walk's single filter. It drops a `system` catalog ref, a
//! view (the catalog reports no location), and a table whose capability
//! manifest marks compute-only (managed default storage), writing the reason to
//! standard error. Eligible refs pass through unchanged, so the stage composes
//! ahead of `credentials get` and a mixed schema keeps going. A drop is the
//! filter working, not a failure: a whole-catalog walk always has tables it
//! cannot read outside compute, so an ineligible table is reported and the stage
//! still succeeds. A check that fails is reported, counted, and the stage exits
//! non-zero after the stream.

use futures_util::stream::StreamExt;
use pqbench::v1::EligibilityReason;
use serde::Serialize;
use serde_json::Value;

use super::StageArgs;
use crate::emit::{Align, Emitter, Row};
use crate::error::{fail_when_dropped, ReadOutcome, RecordError};
use crate::source::{self, dotted_name, read_input, table_ref};
use crate::CliError;

pub(super) async fn run(workspace: &pqbench::Workspace, args: &StageArgs) -> Result<(), CliError> {
    let input = read_input("credentials check").await?;
    if !input.piped {
        return Err("credentials check reads table refs on standard input".into());
    }
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    let records = source::records("credentials check", input.first, input.lines);
    let mut tables = 0;
    let mut ineligible = 0;
    let mut unread = 0;
    let mut checks = records
        .map(|record| async {
            let record = record?;
            let name = table_ref("credentials check", &record)?;
            let outcome = match check(workspace, &name).await {
                Ok(reason) => ReadOutcome::Record((record, reason)),
                Err(error) => {
                    ReadOutcome::Dropped(RecordError::new("table", dotted_name(&name), error))
                }
            };
            Ok::<_, CliError>(outcome)
        })
        .buffer_unordered(args.fan_out.max(1));
    while let Some(outcome) = checks.next().await {
        match outcome? {
            ReadOutcome::Record((record, None)) => {
                emit.write_row(&CheckedRef { record }).await?;
                tables += 1;
            }
            ReadOutcome::Record((_, Some(reason))) => {
                eprintln!("error: {reason}");
                ineligible += 1;
            }
            ReadOutcome::Dropped(error) => {
                error.report();
                unread += 1;
            }
        }
    }
    emit.finish(&format!("tables: {tables}\n")).await?;
    // A drop is the filter working, not a failure: the walk composes on this
    // stage for a mixed schema, and a whole-catalog walk always has tables
    // (system, views, managed default storage) it cannot read outside compute.
    // An ineligible table is reported and the stage succeeds; a check that fails
    // is reported and counted, and the stage exits non-zero after the stream.
    if ineligible > 0 {
        eprintln!("{ineligible} table(s) are not readable outside Databricks compute");
    }
    fail_when_dropped(&[("table", unread)])
}

/// The reason a table is not readable outside Databricks compute, `None` when
/// it is. The core check owns the dialect — Unity reads the capability
/// manifest; Iceberg REST reads its metadata inline through the catalog, so it
/// passes — and the CLI only renders the reason.
async fn check(workspace: &pqbench::Workspace, name: &str) -> Result<Option<String>, CliError> {
    let eligibility = pqbench::credentials::check::check(workspace, name).await?;
    if eligibility.eligible {
        return Ok(None);
    }
    Ok(Some(ineligible(&dotted_name(name), eligibility.reason)))
}

/// The message an ineligible table reports.
fn ineligible(name: &str, reason: Option<EligibilityReason>) -> String {
    let because = match reason {
        Some(EligibilityReason::SystemTable) => "a system table",
        Some(EligibilityReason::NotATable) => "a view, not a table",
        Some(EligibilityReason::NoExternalRead) => {
            "a table only Databricks compute reads (managed default storage)"
        }
        Some(_) | None => "not readable outside Databricks compute",
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
