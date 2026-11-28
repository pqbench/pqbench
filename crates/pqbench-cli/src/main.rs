//! `pqbench`: the command-line frontend's entry point.
//!
//! The command definitions and dispatch live in the library so the docs
//! generator can read the clap tree without a user-facing command.

use std::process::ExitCode;

fn main() -> ExitCode {
    disable_instance_metadata();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the Tokio runtime builds");
    runtime.block_on(pqbench_cli::run())
}

/// Keep the AWS SDK's config chain off the EC2 instance metadata endpoint.
///
/// pqbench reads tables from outside Databricks with explicit credentials
/// (vended, or on the lake source); it never relies on EC2 instance metadata.
/// The SDK's config chain probes IMDS for the region otherwise, and hangs
/// where that endpoint is blackholed. A caller that sets
/// `AWS_EC2_METADATA_DISABLED` wins.
///
/// This runs before the runtime starts, so the process environment is mutated
/// while the process is still single-threaded.
fn disable_instance_metadata() {
    if std::env::var_os("AWS_EC2_METADATA_DISABLED").is_none() {
        std::env::set_var("AWS_EC2_METADATA_DISABLED", "true");
    }
}
