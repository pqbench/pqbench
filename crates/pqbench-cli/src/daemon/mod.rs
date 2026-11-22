//! The catalog daemon: one process per identity, serving catalog records and
//! vended storage leases to every pipeline stage over a local socket.
//!
//! Every catalog call goes through it: a reader asks the daemon, which paces
//! the catalog and keeps it from seeing the same table once per stage, and a
//! reader with no daemon to ask fails with a hint to run `daemon start`. A
//! read of local files names no endpoint and needs no daemon.

use std::path::PathBuf;
use std::time::Duration;

use clap::{Args, Subcommand};

use pqbench::Workspace;

use crate::CliError;

mod client;
mod drive;
pub(crate) mod master;
mod path;
mod process;
pub(crate) mod protocol;
mod rate;
mod serve;
mod session;
mod start;
mod web;

pub(crate) use session::Daemon;

/// Arguments for `daemon`.
#[derive(Args)]
pub(crate) struct DaemonArgs {
    #[command(subcommand)]
    command: DaemonCommand,
}

#[derive(Subcommand)]
enum DaemonCommand {
    /// Start the daemon in the background
    Start(StartArgs),
    /// Stop the daemon
    Stop,
    /// Report whether the daemon is running
    Status,
    /// Serve leases in the foreground
    Serve(ServeArgs),
}

#[derive(Args)]
struct StartArgs {
    /// seconds without a connection before the daemon exits
    #[arg(long, default_value_t = 900)]
    idle_seconds: u64,
    #[command(flatten)]
    master: MasterArgs,
}

/// The master: the map of partition versions, and what the daemon runs to
/// move them.
#[derive(Args, Clone)]
pub(crate) struct MasterArgs {
    /// also run the master: the partition versions and the commands that move them
    #[arg(long, requires = "store")]
    master: bool,
    /// the master's store: its checkpoint and results (with --master)
    #[arg(long, requires = "master", value_name = "DIR")]
    store: Option<PathBuf>,
    /// walk this catalog or schema (`catalogs/C`, `catalogs/C/schemas/S`); repeatable
    #[arg(long, requires = "master", value_name = "NAME", value_parser = walk_root)]
    walk: Vec<String>,
    /// walk the tables these Table records name, as they are (e.g. local tables)
    #[arg(long, requires = "master", value_name = "FILE")]
    walk_tables: Option<PathBuf>,
    /// seconds from one walk to the next; without it the master walks once, when it starts
    #[arg(long, requires = "master", value_name = "SECONDS")]
    walk_period_seconds: Option<u64>,
    /// run this step on the versions: price, download, bytemass or lint; repeatable
    #[arg(long = "step", requires = "master", value_name = "STEP", value_parser = step_name)]
    steps: Vec<String>,
    /// the most step commands running at once
    #[arg(long, requires = "master", default_value_t = 8)]
    processes: usize,
    /// kill a step command that runs longer, and count it as a failure
    #[arg(long, requires = "master", value_name = "SECONDS")]
    step_time_limit_seconds: Option<u64>,
    /// also serve the web app: the master's records and status page (with --master)
    #[arg(long, requires = "master")]
    web_ui: bool,
    /// the web app's address (with --web-ui)
    #[arg(long, value_name = "ADDR", default_value = "127.0.0.1:8180")]
    listen: String,
}

/// A walk root: a catalog or a schema name.
fn walk_root(value: &str) -> Result<String, String> {
    let segments: Vec<&str> = value.trim_end_matches('/').split('/').collect();
    match segments.as_slice() {
        ["catalogs", catalog] | ["catalogs", catalog, "schemas", _]
            if !catalog.is_empty() && segments.iter().all(|segment| !segment.is_empty()) =>
        {
            Ok(segments.join("/"))
        }
        _ => Err(format!(
            "{value} is not a catalog or schema name (catalogs/C or catalogs/C/schemas/S)"
        )),
    }
}

/// A step's name.
fn step_name(value: &str) -> Result<String, String> {
    match master::STEPS.iter().find(|step| step.name == value) {
        Some(step) => Ok(step.name.to_string()),
        None => Err(format!(
            "{value} is not a step: {}",
            master::STEPS
                .iter()
                .map(|step| step.name)
                .collect::<Vec<_>>()
                .join(", ")
        )),
    }
}

impl MasterArgs {
    /// What the driver runs.
    fn settings(&self) -> drive::Settings {
        drive::Settings {
            walk: self.walk.clone(),
            walk_tables: self.walk_tables.as_deref().map(path::absolute),
            walk_period: self.walk_period_seconds.map(Duration::from_secs),
            steps: master::STEPS
                .iter()
                .filter(|step| self.steps.iter().any(|name| name == step.name))
                .collect(),
            processes: self.processes.max(1),
            time_limit: self.step_time_limit_seconds.map(Duration::from_secs),
        }
    }

    /// The flags again, for the background daemon's command line.
    pub(crate) fn arguments(&self) -> Vec<std::ffi::OsString> {
        let mut arguments: Vec<std::ffi::OsString> = Vec::new();
        let Some(store) = self.store() else {
            return arguments;
        };
        arguments.extend(["--master".into(), "--store".into(), store.into()]);
        for root in &self.walk {
            arguments.extend(["--walk".into(), root.into()]);
        }
        if let Some(tables) = &self.walk_tables {
            arguments.extend(["--walk-tables".into(), path::absolute(tables).into()]);
        }
        if let Some(seconds) = self.walk_period_seconds {
            arguments.extend(["--walk-period-seconds".into(), seconds.to_string().into()]);
        }
        for step in &self.steps {
            arguments.extend(["--step".into(), step.into()]);
        }
        arguments.extend(["--processes".into(), self.processes.to_string().into()]);
        if let Some(seconds) = self.step_time_limit_seconds {
            arguments.extend([
                "--step-time-limit-seconds".into(),
                seconds.to_string().into(),
            ]);
        }
        if let Some(address) = self.web_ui() {
            arguments.extend(["--web-ui".into(), "--listen".into(), address.into()]);
        }
        arguments
    }

    /// The web app's address, when it runs.
    pub(crate) fn web_ui(&self) -> Option<&str> {
        (self.master && self.web_ui).then_some(self.listen.as_str())
    }

    /// The store, when the master runs.
    pub(crate) fn store(&self) -> Option<PathBuf> {
        self.master
            .then(|| self.store.as_deref().map(path::absolute))
            .flatten()
    }
}

#[derive(Args)]
struct ServeArgs {
    /// socket path (default: the user's runtime directory)
    #[arg(long)]
    socket: Option<PathBuf>,
    /// seconds without a connection before the daemon exits; 0 serves until stopped
    #[arg(long, default_value_t = 0)]
    idle_seconds: u64,
    #[command(flatten)]
    master: MasterArgs,
}

impl DaemonArgs {
    /// Whether the command only needs `PQB_DAEMON_SOCK` (stop/status with an
    /// explicit socket path), so it can run without a profile or endpoint.
    pub(crate) fn socket_only(&self) -> bool {
        matches!(self.command, DaemonCommand::Stop | DaemonCommand::Status)
            && std::env::var_os("PQB_DAEMON_SOCK").is_some()
    }

    /// Whether the command needs a catalog endpoint. Start and serve always
    /// do; stop and status can find an explicit socket without one.
    pub(crate) fn requires_endpoint(&self) -> bool {
        match &self.command {
            DaemonCommand::Start(_) | DaemonCommand::Serve(_) => true,
            DaemonCommand::Stop | DaemonCommand::Status => !self.socket_only(),
        }
    }
}

pub(crate) async fn run(workspace: &Workspace, args: &DaemonArgs) -> Result<(), CliError> {
    let socket = path::socket_path(workspace.config());
    match &args.command {
        DaemonCommand::Start(args) => {
            start::start(workspace.config(), &socket, args.idle_seconds, &args.master).await?;
        }
        DaemonCommand::Stop => start::stop(&socket).await?,
        DaemonCommand::Status => start::status(&socket).await?,
        DaemonCommand::Serve(args) => {
            let socket = args.socket.as_deref().map_or(socket, path::absolute);
            let session = Daemon::direct(workspace.clone());
            let master = match args.master.store() {
                Some(store) => Some(std::sync::Arc::new(std::sync::RwLock::new(
                    master::Master::open(&store)?,
                ))),
                None => None,
            };
            if let (Some(address), Some(master)) = (args.master.web_ui(), &master) {
                let listener = web::bind(address).await?;
                tokio::spawn(web::run(listener, std::sync::Arc::clone(master)));
            }
            if let Some(master) = &master {
                drive::spawn(
                    std::sync::Arc::clone(master),
                    args.master.settings(),
                    socket.clone(),
                );
            }
            serve::run(session, master, socket, args.idle_seconds).await?;
        }
    }
    Ok(())
}
