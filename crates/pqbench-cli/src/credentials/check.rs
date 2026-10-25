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

use pqbench::v1;
use pqbench::v1::EligibilityReason;

use super::StageArgs;
use crate::parse;
use crate::source::{drive, filter_map};
use crate::CliError;

pub(crate) async fn run(workspace: &pqbench::Workspace, args: &StageArgs) -> Result<(), CliError> {
    let parents = parse::parents::<v1::Table>("credentials check").await?;
    drive(filter_map(
        parents,
        args.fan_out,
        |table: v1::Table| async move {
            match check(workspace, &table.name).await {
                Ok(None) => Ok(Some(table)),
                Ok(Some(reason)) => {
                    eprintln!("error: {reason}");
                    Ok(None)
                }
                Err(cause) => Err(cause),
            }
        },
    ))
    .await
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
    Ok(Some(ineligible(name, eligibility.reason)))
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
