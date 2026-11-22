//! `job`: the processes that move partition versions through the master.
//!
//! A job is a client of the daemon's master: it claims work, runs one plain
//! pqbench command per input with its standard output going to the path the
//! master gave, and commits. The commands do not know the master; running
//! one by hand is the same command. One instance of each job runs at a time
//! (a lock beside the daemon's socket, released when the process dies); a
//! time limit is the scheduler's (`timeout 3600 pqbench job scan`).

pub(crate) mod scan;
mod step;

use std::fs::{File, OpenOptions};
use std::io::Write as _;
use std::os::fd::AsRawFd;
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Command, Stdio};

use clap::{Args, Subcommand};

use crate::daemon::protocol::Request;
use crate::CliError;

/// Arguments for `job`.
#[derive(Args)]
pub(crate) struct JobArgs {
    #[command(subcommand)]
    job: Job,
}

#[derive(Subcommand)]
enum Job {
    /// List each table's partitions into the master (Table records on stdin)
    Scan(scan::ScanArgs),
    /// Price the pending versions (`get partition-price`)
    Price(step::StepArgs),
    /// Measure each priced version's sample file (`bytemass`)
    Bytemass(step::StepArgs),
    /// List each measured version's storage suggestions (`list suggestions`)
    Lint(step::StepArgs),
    /// Delete the results and rows of the versions the scan no longer finds
    Purge,
}

/// Run one job.
pub(crate) async fn run(daemon: &crate::daemon::Daemon, args: &JobArgs) -> Result<(), CliError> {
    match &args.job {
        Job::Scan(args) => scan::run(daemon, args).await,
        Job::Price(args) => step::run(daemon, &step::PRICE, args).await,
        Job::Bytemass(args) => step::run(daemon, &step::BYTEMASS, args).await,
        Job::Lint(args) => step::run(daemon, &step::LINT, args).await,
        Job::Purge => {
            let _lock = lock(daemon, "purge")?;
            daemon.master(&Request::Purge).await?;
            Ok(())
        }
    }
}

impl JobArgs {
    /// The command's name, used in configuration errors.
    pub(crate) fn name(&self) -> &'static str {
        match self.job {
            Job::Scan(_) => "job scan",
            Job::Price(_) => "job price",
            Job::Bytemass(_) => "job bytemass",
            Job::Lint(_) => "job lint",
            Job::Purge => "job purge",
        }
    }
}

/// Hold the job's lock for as long as the returned file is open: another
/// instance of the same job against the same daemon is refused.
fn lock(daemon: &crate::daemon::Daemon, job: &str) -> Result<File, CliError> {
    let socket = daemon.socket().ok_or_else(|| -> CliError {
        "a job needs the daemon's master; run `pqbench daemon start --master --store DIR`".into()
    })?;
    let path = socket.with_extension(format!("{job}.lock"));
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)
        .map_err(|error| format!("cannot open {}: {error}", path.display()))?;
    // SAFETY: flock on a descriptor this process owns; it is released when
    // the file closes or the process dies.
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        return Err(format!("another `job {job}` is running ({})", path.display()).into());
    }
    Ok(file)
}

/// Run `pqbench ARGS` once, with `input` on its standard input and its
/// standard output written to `output`. A failure is the command's standard
/// error. The child dies with the job, so a job killed for its time limit
/// leaves no command running. Blocking: call from `spawn_blocking`.
fn run_command(args: &[&str], input: &str, output: &Path, socket: &Path) -> Result<(), String> {
    let exe = std::env::current_exe().map_err(|error| error.to_string())?;
    let stdout = File::create(output)
        .map_err(|error| format!("cannot create {}: {error}", output.display()))?;
    let mut command = Command::new(exe);
    command
        .args(args)
        .env("PQB_DAEMON_SOCK", socket)
        .stdin(Stdio::piped())
        .stdout(stdout)
        .stderr(Stdio::piped());
    // SAFETY: `prctl` is async-signal-safe, so it may run between fork and
    // exec.
    unsafe {
        command.pre_exec(|| {
            if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = command
        .spawn()
        .map_err(|error| format!("cannot run pqbench: {error}"))?;
    let written = child
        .stdin
        .take()
        .map(|mut stdin| stdin.write_all(input.as_bytes()));
    let mut stderr = String::new();
    if let Some(mut pipe) = child.stderr.take() {
        let _ = std::io::Read::read_to_string(&mut pipe, &mut stderr);
    }
    let status = child.wait().map_err(|error| error.to_string())?;
    if let Some(Err(error)) = written {
        return Err(format!("cannot write the command's input: {error}"));
    }
    if status.success() {
        Ok(())
    } else {
        let stderr = stderr.trim();
        Err(match stderr.is_empty() {
            true => format!("`pqbench {}` failed: {status}", args.join(" ")),
            false => stderr.to_string(),
        })
    }
}
