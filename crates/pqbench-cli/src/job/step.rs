//! `job price`, `job bytemass`, `job lint`: one step of the pipeline each.
//!
//! A run returns its own leftover claims, then claims a batch at a time,
//! runs the step's command once per version, and commits each: exit 0 is a
//! success (no output is an empty result), anything else a failure with the
//! command's standard error. A version that failed twice is left out of
//! claims, so the run ends.

use clap::Args;
use futures_util::StreamExt;

use crate::daemon::master::{Claim, State};
use crate::daemon::protocol::Request;
use crate::CliError;

/// Arguments for a step job.
#[derive(Args)]
pub(crate) struct StepArgs {
    /// versions in flight at once
    #[arg(long, default_value_t = 8)]
    fan_out: usize,
}

/// One step: where it claims from, the job state it holds, where a success
/// goes, and the command it runs on each claimed version.
pub(crate) struct Step {
    name: &'static str,
    source: State,
    target: State,
    done: State,
    command: &'static [&'static str],
    /// The command's standard input for a version, or `None` when the version
    /// skips the command and goes straight to `UP_TO_DATE`.
    input: fn(&Claim) -> Option<String>,
}

pub(crate) const PRICE: Step = Step {
    name: "price",
    source: State::Pending,
    target: State::Pricing,
    done: State::Priced,
    command: &["get", "partition-price"],
    input: partition_input,
};

pub(crate) const BYTEMASS: Step = Step {
    name: "bytemass",
    source: State::Priced,
    target: State::Bytemassing,
    done: State::Bytemassed,
    command: &["bytemass"],
    input: readable_sample_input,
};

pub(crate) const LINT: Step = Step {
    name: "lint",
    source: State::Bytemassed,
    target: State::Linting,
    done: State::UpToDate,
    command: &["list", "suggestions"],
    input: sample_input,
};

/// Classes whose objects cannot be read without a restore.
const ARCHIVE_CLASSES: &[&str] = &["GLACIER", "DEEP_ARCHIVE"];

fn partition_input(claim: &Claim) -> Option<String> {
    Some(format!("{}\n", claim.partition))
}

fn sample_input(claim: &Claim) -> Option<String> {
    let sample = claim.partition.get("sample_file")?;
    Some(format!("{sample}\n"))
}

/// The sample file, unless the price saw an archive class: such a version is
/// done without a read (a GET would need a restore).
fn readable_sample_input(claim: &Claim) -> Option<String> {
    let archived = claim.results.get("price").is_some_and(|price| {
        price["storage_classes"].as_array().is_some_and(|classes| {
            classes
                .iter()
                .any(|class| ARCHIVE_CLASSES.contains(&class.as_str().unwrap_or_default()))
        })
    });
    if archived {
        return None;
    }
    sample_input(claim)
}

/// Run one step until nothing is left to claim.
pub(crate) async fn run(
    daemon: &crate::daemon::Daemon,
    step: &Step,
    args: &StepArgs,
) -> Result<(), CliError> {
    let _lock = super::lock(daemon, step.name)?;
    let socket = daemon
        .socket()
        .expect("the lock found the socket")
        .to_path_buf();
    daemon
        .master(&Request::Revert { state: step.target })
        .await?;
    let fan_out = args.fan_out.max(1);
    let mut failed = 0;
    loop {
        let response = daemon
            .master(&Request::Claim {
                source: step.source,
                target: step.target,
                limit: fan_out * 4,
            })
            .await?;
        if response.records.is_empty() {
            break;
        }
        let claims: Vec<Claim> = response
            .records
            .into_iter()
            .map(serde_json::from_value)
            .collect::<Result<_, _>>()?;
        let results: Vec<Result<(), CliError>> = futures_util::stream::iter(claims)
            .map(|claim| run_one(daemon, step, &socket, claim))
            .buffer_unordered(fan_out)
            .collect()
            .await;
        for result in results {
            if let Err(error) = result {
                crate::error::report(&error);
                failed += 1;
            }
        }
    }
    match failed {
        0 => Ok(()),
        failed => Err(format!("job {}: {failed} version(s) failed", step.name).into()),
    }
}

/// Run the step's command on one claimed version and commit it.
async fn run_one(
    daemon: &crate::daemon::Daemon,
    step: &Step,
    socket: &std::path::Path,
    claim: Claim,
) -> Result<(), CliError> {
    let (target, error) = match (step.input)(&claim) {
        None => (State::UpToDate, None),
        Some(input) => {
            let path = claim.path.clone();
            let socket = socket.to_path_buf();
            let command = step.command;
            let outcome = tokio::task::spawn_blocking(move || {
                super::run_command(command, &input, &path, &socket)
            })
            .await
            .map_err(|error| error.to_string())?;
            (step.done, outcome.err())
        }
    };
    daemon
        .master(&Request::Commit {
            name: claim.name.clone(),
            etag: claim.etag.clone(),
            source: step.target,
            target,
            error: error.clone(),
        })
        .await?;
    match error {
        None => Ok(()),
        Some(error) => Err(crate::error::context(&claim.name, error)),
    }
}
