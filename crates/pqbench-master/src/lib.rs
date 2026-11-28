//! The partition-version state engine (master) for pqbench: a process of its
//! own, with its own socket and its own store.
//!
//! The master holds the map of partition versions and runs the pqbench
//! commands that move them (`get partition-price`, `download`, `bytemass`,
//! `list suggestions`), walking the tables on a period. A client reads the
//! map and one version's results over [`protocol`] on the socket [`socket`]
//! names; [`client`] is that side.
//!
//! The commands themselves talk to the catalog daemon, not to the master, so
//! a master without a daemon still answers for the map it has.

pub mod client;
pub mod protocol;
pub mod socket;

mod drive;
mod master;
mod process;
mod serve;
mod start;
mod store;

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Args, Parser, Subcommand};

use drive::Settings;

/// The master: the map of partition versions, and the commands that move them.
#[derive(Parser)]
#[command(
    name = "pqbench-master",
    version,
    about = "Partition-version state engine (master) for pqbench."
)]
pub(crate) struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Start the master in the background
    Start(RunArgs),
    /// Serve the master in the foreground
    Serve(RunArgs),
    /// Stop the master
    Stop(TargetArgs),
    /// Report whether the master is running
    Status(TargetArgs),
}

#[derive(Args)]
pub(crate) struct RunArgs {
    /// the master's store: its checkpoint, results, and default socket
    #[arg(long, value_name = "DIR")]
    pub store: PathBuf,
    /// the master's socket (default: the store's master.sock)
    #[arg(long, env = "PQB_MASTER_SOCK", value_name = "PATH")]
    pub socket: Option<PathBuf>,
    /// the daemon socket the worker commands talk to
    #[arg(long, value_name = "PATH")]
    pub daemon_socket: Option<PathBuf>,
    /// the pqbench binary the worker commands run (default: the one beside this executable)
    #[arg(long, value_name = "BIN")]
    pub pqbench: Option<PathBuf>,
    /// walk the tables these Table records name instead of the metastore (e.g. local tables)
    #[arg(long, value_name = "FILE")]
    pub walk_tables: Option<PathBuf>,
    /// seconds from one walk to the next; without it the master walks once, when it starts
    #[arg(long, value_name = "SECONDS")]
    pub walk_period_seconds: Option<u64>,
    /// the host has little network: copy no file; bytemass reads each sample file's footer in place, and there is no lint
    #[arg(long)]
    pub low_network: bool,
}

impl RunArgs {
    /// The store, absolute so a detached master resolves it from any working
    /// directory.
    fn store(&self) -> PathBuf {
        socket::absolute(&self.store)
    }

    /// What the driver runs.
    fn settings(&self) -> Settings {
        Settings {
            walk_tables: self.walk_tables.as_deref().map(socket::absolute),
            walk_period: self.walk_period_seconds.map(Duration::from_secs),
        }
    }
}

#[derive(Args)]
pub(crate) struct TargetArgs {
    /// the master's socket (default: PQB_MASTER_SOCK, else the store's master.sock)
    #[arg(long, env = "PQB_MASTER_SOCK", value_name = "PATH")]
    pub socket: Option<PathBuf>,
    /// the master's store, for the socket beside it
    #[arg(long, value_name = "DIR")]
    pub store: Option<PathBuf>,
}

/// Run the command line: the master's command, or the error that stopped it.
pub async fn run() -> ExitCode {
    let cli = Cli::parse();
    let result = match &cli.command {
        Command::Start(args) => start::start(args).await,
        Command::Serve(args) => serve::serve(args).await,
        Command::Stop(args) => start::stop(args).await,
        Command::Status(args) => start::status(args).await,
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("pqbench-master: {error}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;
    use std::path::Path;

    /// The flags the background master is started with are the flags it
    /// accepts: a round-trip through the parser.
    #[test]
    fn the_run_flags_parse_from_the_start_command_line() {
        let arguments: Vec<OsString> = [
            "pqbench-master",
            "serve",
            "--store",
            "/tmp/store",
            "--socket",
            "/tmp/master.sock",
            "--daemon-socket",
            "/tmp/daemon.sock",
            "--pqbench",
            "/tmp/pqbench",
            "--walk-tables",
            "/tmp/tables.ndjson",
            "--walk-period-seconds",
            "60",
            "--low-network",
        ]
        .into_iter()
        .map(OsString::from)
        .collect();
        let cli = Cli::try_parse_from(arguments).expect("the documented flags parse");
        let Command::Serve(args) = cli.command else {
            panic!("serve is a subcommand");
        };
        assert_eq!(args.store, PathBuf::from("/tmp/store"));
        assert_eq!(args.socket.as_deref(), Some(Path::new("/tmp/master.sock")));
        assert_eq!(
            args.daemon_socket.as_deref(),
            Some(Path::new("/tmp/daemon.sock"))
        );
        assert_eq!(args.pqbench.as_deref(), Some(Path::new("/tmp/pqbench")));
        assert_eq!(
            args.walk_tables.as_deref(),
            Some(Path::new("/tmp/tables.ndjson"))
        );
        assert_eq!(args.walk_period_seconds, Some(60));
        assert_eq!(args.settings().walk_period, Some(Duration::from_secs(60)));
        assert!(args.low_network);
    }
}
