//! `pqbench`: the command-line frontend's entry point.
//!
//! The command definitions and dispatch live in the library so the docs
//! generator can read the clap tree without a user-facing command.

use std::process::ExitCode;

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    pqbench_cli::run().await
}
