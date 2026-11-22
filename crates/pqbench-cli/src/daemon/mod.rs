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
mod store;

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
    /// walk the tables these Table records name instead of the metastore (e.g. local tables)
    #[arg(long, requires = "master", value_name = "FILE")]
    walk_tables: Option<PathBuf>,
    /// seconds from one walk to the next; without it the master walks once, when it starts
    #[arg(long, requires = "master", value_name = "SECONDS")]
    walk_period_seconds: Option<u64>,
    /// the host has little network: copy no file; bytemass reads each sample file's footer in place, and there is no lint
    #[arg(long, requires = "master")]
    low_network: bool,
}

impl MasterArgs {
    /// What the driver runs.
    fn settings(&self) -> drive::Settings {
        drive::Settings {
            walk_tables: self.walk_tables.as_deref().map(path::absolute),
            walk_period: self.walk_period_seconds.map(Duration::from_secs),
        }
    }

    /// The flags again, for the background daemon's command line.
    pub(crate) fn arguments(&self) -> Vec<std::ffi::OsString> {
        let mut arguments: Vec<std::ffi::OsString> = Vec::new();
        let Some(store) = self.store() else {
            return arguments;
        };
        arguments.extend(["--master".into(), "--store".into(), store.into()]);
        if let Some(tables) = &self.walk_tables {
            arguments.extend(["--walk-tables".into(), path::absolute(tables).into()]);
        }
        if let Some(seconds) = self.walk_period_seconds {
            arguments.extend(["--walk-period-seconds".into(), seconds.to_string().into()]);
        }
        if self.low_network {
            arguments.push("--low-network".into());
        }
        arguments
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
                Some(root) => {
                    let store = store::Store::open(&root)?;
                    let master =
                        master::Master::new(store.read_checkpoint()?, args.master.low_network)?;
                    store.sweep(|key| master.state(key))?;
                    let master = std::sync::Arc::new(std::sync::RwLock::new(master));
                    let settings = args.master.settings();
                    drive::spawn(master.clone(), store.clone(), settings, socket.clone());
                    Some(serve::SharedMaster { master, store })
                }
                None => None,
            };
            serve::run(session, master, socket, args.idle_seconds).await?;
        }
    }
    Ok(())
}
