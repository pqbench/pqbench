//! The lease daemon: one process per identity, serving vended storage leases
//! to every pipeline stage over a local socket.
//!
//! The daemon is optional. A reader with no socket vends directly, so a bare
//! `pqbench list files` and a scheduler task still work; `daemon start` only
//! keeps the catalog from seeing the same table once per stage.

use std::path::PathBuf;

use clap::{Args, Subcommand};

use crate::CliError;
use pqbench::Workspace;

mod cache;
mod client;
mod path;
mod protocol;
mod rate;
mod serve;
mod start;

pub(crate) use cache::LeaseCache;

/// Arguments for `daemon`.
#[derive(Args)]
pub(crate) struct DaemonArgs {
    #[command(subcommand)]
    command: DaemonCommand,
}

#[derive(Subcommand)]
enum DaemonCommand {
    /// Start the lease daemon in the background
    Start(StartArgs),
    /// Stop the lease daemon
    Stop,
    /// Report whether the lease daemon is running
    Status,
    /// Serve leases in the foreground
    Serve(ServeArgs),
}

#[derive(Args)]
struct StartArgs {
    /// seconds without a connection before the daemon exits
    #[arg(long, default_value_t = 900)]
    idle_seconds: u64,
}

#[derive(Args)]
struct ServeArgs {
    /// socket path (default: the user's runtime directory)
    #[arg(long)]
    socket: Option<PathBuf>,
    /// seconds without a connection before the daemon exits; 0 serves until stopped
    #[arg(long, default_value_t = 0)]
    idle_seconds: u64,
}

impl DaemonArgs {
    /// Whether the command only needs `PQB_DAEMON_SOCK` (stop/status with an
    /// explicit socket path), so it can run without a profile or endpoint.
    pub(crate) fn socket_only(&self) -> bool {
        matches!(self.command, DaemonCommand::Stop | DaemonCommand::Status)
            && std::env::var_os("PQB_DAEMON_SOCK").is_some()
    }

    /// Whether the command needs a catalog endpoint. With an explicit socket,
    /// stop and status can find the daemon without one; start and serve cannot.
    pub(crate) fn requires_endpoint(&self) -> bool {
        match &self.command {
            DaemonCommand::Start(_) | DaemonCommand::Serve(_) => true,
            DaemonCommand::Stop | DaemonCommand::Status => {
                std::env::var_os("PQB_DAEMON_SOCK").is_none()
            }
        }
    }
}

pub(crate) async fn run(workspace: &Workspace, args: &DaemonArgs) -> Result<(), CliError> {
    let socket = path::socket_path(workspace.config());
    match &args.command {
        DaemonCommand::Start(args) => {
            start::start(workspace.config(), &socket, args.idle_seconds).await?;
        }
        DaemonCommand::Stop => start::stop(&socket).await?,
        DaemonCommand::Status => start::status(&socket).await?,
        DaemonCommand::Serve(args) => {
            let socket = args.socket.as_deref().map_or(socket, path::absolute);
            let cache = LeaseCache::direct(workspace.clone());
            serve::run(cache, socket, args.idle_seconds).await?;
        }
    }
    Ok(())
}
