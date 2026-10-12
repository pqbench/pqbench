//! `pqbench`: the command-line frontend.

use std::ffi::OsString;
use std::process::ExitCode;

use clap::{CommandFactory, Parser, Subcommand};

mod bench;
mod bytemass;
mod catalog;
mod compression;
mod config;
mod credentials;
mod diff;
mod document;
mod emit;
mod experiment;
mod file;
mod help;
mod lz;
mod metastore;
mod partition;
mod profile;
mod ratelimit;
mod render;
mod rows;
mod schema;
mod setup;
mod skill;
mod source;
mod table;
mod viz;

/// The CLI's single error channel: any error from the io, parquet, or codec
/// layers, converted via `?`.
pub(crate) type CliError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Parser)]
#[command(
    name = "pqbench",
    about = help::ROOT_ABOUT,
    long_about = help::ROOT_LONG_ABOUT,
    after_help = help::ROOT_AFTER,
    after_long_help = help::ROOT_AFTER
)]
struct Cli {
    /// The configuration every command shares (flags, with a `PQB_*` fallback).
    #[command(flatten)]
    config: config::ConfigArgs,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Diff(diff::DiffArgs),
    #[command(
        about = help::LZ_ABOUT,
        long_about = help::LZ_LONG_ABOUT,
        after_help = help::LZ_AFTER,
        after_long_help = help::LZ_AFTER
    )]
    Lz(bench::BenchArgs),
    #[command(
        about = help::COMPRESSION_ABOUT,
        long_about = help::COMPRESSION_LONG_ABOUT,
        after_help = help::COMPRESSION_AFTER,
        after_long_help = help::COMPRESSION_AFTER
    )]
    Compression(compression::CompressionArgs),
    #[command(
        about = help::BYTEMASS_ABOUT,
        long_about = help::BYTEMASS_LONG_ABOUT,
        after_help = help::BYTEMASS_AFTER,
        after_long_help = help::BYTEMASS_AFTER
    )]
    Bytemass(bytemass::BytemassArgs),
    #[command(
        about = help::TABLE_ABOUT,
        long_about = help::TABLE_LONG_ABOUT,
        after_help = help::TABLE_AFTER,
        after_long_help = help::TABLE_AFTER
    )]
    Table(table::TableArgs),
    #[command(
        about = help::METASTORE_ABOUT,
        long_about = help::METASTORE_LONG_ABOUT,
        after_help = help::METASTORE_AFTER,
        after_long_help = help::METASTORE_AFTER
    )]
    Metastore(metastore::MetastoreArgs),
    #[command(
        about = help::CATALOG_ABOUT,
        long_about = help::CATALOG_LONG_ABOUT,
        after_help = help::CATALOG_AFTER,
        after_long_help = help::CATALOG_AFTER
    )]
    Catalog(catalog::CatalogArgs),
    #[command(
        about = help::SCHEMA_ABOUT,
        long_about = help::SCHEMA_LONG_ABOUT,
        after_help = help::SCHEMA_AFTER,
        after_long_help = help::SCHEMA_AFTER
    )]
    Schema(schema::SchemaArgs),
    #[command(
        about = help::CREDENTIALS_ABOUT,
        long_about = help::CREDENTIALS_LONG_ABOUT,
        after_help = help::CREDENTIALS_AFTER,
        after_long_help = help::CREDENTIALS_AFTER
    )]
    Credentials(credentials::CredentialsArgs),
    #[command(
        about = help::PARTITION_ABOUT,
        long_about = help::PARTITION_LONG_ABOUT,
        after_help = help::PARTITION_AFTER,
        after_long_help = help::PARTITION_AFTER
    )]
    Partition(partition::PartitionArgs),
    #[command(
        about = help::FILE_ABOUT,
        long_about = help::FILE_LONG_ABOUT,
        after_help = help::FILE_AFTER,
        after_long_help = help::FILE_AFTER
    )]
    File(file::FileArgs),
    #[command(
        name = "ratelimit",
        about = help::RATELIMIT_ABOUT,
        long_about = help::RATELIMIT_LONG_ABOUT,
        after_help = help::RATELIMIT_AFTER,
        after_long_help = help::RATELIMIT_AFTER
    )]
    RateLimit(ratelimit::RateLimitArgs),
    #[command(
        about = help::PROFILE_ABOUT,
        long_about = help::PROFILE_LONG_ABOUT,
        after_help = help::PROFILE_AFTER,
        after_long_help = help::PROFILE_AFTER
    )]
    Profile(profile::ProfileArgs),
    #[command(
        about = help::EXPERIMENT_ABOUT,
        long_about = help::EXPERIMENT_LONG_ABOUT,
        after_help = help::EXPERIMENT_AFTER,
        after_long_help = help::EXPERIMENT_AFTER
    )]
    Experiment(experiment::ExperimentArgs),
    #[command(
        about = help::SKILL_ABOUT,
        long_about = help::SKILL_LONG_ABOUT,
        after_help = help::SKILL_AFTER,
        after_long_help = help::SKILL_AFTER
    )]
    Skill(skill::SkillArgs),
    #[command(
        about = help::VIZ_ABOUT,
        long_about = help::VIZ_LONG_ABOUT,
        after_help = help::VIZ_AFTER,
        after_long_help = help::VIZ_AFTER
    )]
    Viz(viz::VizArgs),
    #[command(
        about = help::SETUP_ABOUT,
        long_about = help::SETUP_LONG_ABOUT,
        after_help = help::SETUP_AFTER,
        after_long_help = help::SETUP_AFTER
    )]
    Setup(setup::SetupArgs),
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    // pqbench reads tables from outside Databricks with explicit credentials
    // (vended, or on the lake source); it never relies on EC2 instance
    // metadata. The AWS SDK's config chain probes IMDS for the region
    // otherwise, and hangs where that endpoint is blackholed. Default the
    // probe off before any command builds an AWS client; a caller that sets
    // AWS_EC2_METADATA_DISABLED wins.
    if std::env::var_os("AWS_EC2_METADATA_DISABLED").is_none() {
        std::env::set_var("AWS_EC2_METADATA_DISABLED", "true");
    }
    let cli = Cli::parse_from(normalize(std::env::args_os()));
    match dispatch(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

/// Rewrite the verb-first spelling of a walk read (`pqbench get table X`) into
/// the noun tree (`pqbench table get X`) before clap parses it, so both
/// spellings run through one command definition.
///
/// The nouns and the value-taking global flags are read back out of the
/// command tree, so a new subcommand or flag needs no change here; the only
/// literals are the two sugar verbs, which have no clap command of their own.
fn normalize(args: impl Iterator<Item = OsString>) -> Vec<OsString> {
    const VERBS: [&str; 2] = ["get", "ls"];
    let root = Cli::command();
    let nouns: Vec<&str> = root
        .get_subcommands()
        .map(clap::Command::get_name)
        .collect();
    let value_flags: Vec<String> = root
        .get_arguments()
        .filter(|arg| arg.get_action().takes_values())
        .filter_map(|arg| arg.get_long().map(|name| format!("--{name}")))
        .collect();

    let mut args: Vec<OsString> = args.collect();
    let mut index = 1;
    while index < args.len() {
        let Some(arg) = args[index].to_str() else {
            break;
        };
        if value_flags.iter().any(|flag| flag == arg) {
            // A value-taking global flag: skip the flag and its value.
            index += 2;
        } else if arg.starts_with('-') {
            // Another flag, or a `--flag=value` pair: skip the flag only.
            index += 1;
        } else {
            break;
        }
    }
    let verb = args
        .get(index)
        .and_then(|arg| arg.to_str())
        .unwrap_or_default();
    if !VERBS.contains(&verb) {
        return args;
    }
    let noun = args
        .get(index + 1)
        .and_then(|arg| arg.to_str())
        .unwrap_or_default();
    if nouns.contains(&noun) {
        args.swap(index, index + 1);
    } else if noun.is_empty() || matches!(noun, "-h" | "--help") {
        // A bare verb asks what it can read/list: show the root page, which
        // names the walk's nouns. Anything else is clap's error to report.
        args[index] = OsString::from("--help");
    }
    args
}

/// Resolve the shared configuration and context once, then run the command.
async fn dispatch(cli: Cli) -> Result<(), CliError> {
    let config = config::resolve(
        cli.command.name(),
        &cli.config,
        cli.command.requires_endpoint(),
    )?;
    let workspace = pqbench::Workspace::new(config)?;
    let lake = pqbench::Lake::new(&workspace);
    match cli.command {
        Command::Diff(args) => diff::run(&lake, &args).await,
        Command::Lz(args) => lz::run(&args).await,
        Command::Compression(args) => compression::run(&args).await,
        Command::Bytemass(args) => bytemass::run(&lake, &args).await,
        Command::Table(args) => table::run(&workspace, &lake, &args).await,
        Command::Metastore(args) => metastore::run(&workspace, &args).await,
        Command::Catalog(args) => catalog::run(&workspace, &args).await,
        Command::Schema(args) => schema::run(&workspace, &args).await,
        Command::Credentials(args) => credentials::run(&workspace, &args).await,
        Command::Partition(args) => partition::run(&lake, &args).await,
        Command::File(args) => file::run(&lake, &args).await,
        Command::RateLimit(args) => ratelimit::run(&args).await,
        Command::Profile(args) => profile::run(&args).await,
        Command::Experiment(args) => experiment::run(&args).await,
        Command::Skill(args) => skill::run(&args),
        Command::Viz(args) => viz::run(&args).await,
        Command::Setup(args) => setup::run(&workspace, &args),
    }
}

impl Command {
    /// The command's name, used in configuration errors.
    fn name(&self) -> &'static str {
        match self {
            Command::Diff(_) => "diff",
            Command::Lz(_) => "lz",
            Command::Compression(_) => "compression",
            Command::Bytemass(_) => "bytemass",
            Command::Table(_) => "table",
            Command::Metastore(_) => "metastore",
            Command::Catalog(_) => "catalog",
            Command::Schema(_) => "schema",
            Command::Credentials(_) => "credentials",
            Command::Partition(_) => "partition",
            Command::File(_) => "file",
            Command::RateLimit(_) => "ratelimit",
            Command::Profile(_) => "profile",
            Command::Experiment(_) => "experiment",
            Command::Skill(_) => "skill",
            Command::Viz(_) => "viz",
            Command::Setup(_) => "setup",
        }
    }

    /// Whether the command needs `PQB_ENDPOINT`. The catalog levels read a
    /// catalog; a storage-only command (`partition ls`, `file ls`, `bytemass`,
    /// …) does not.
    fn requires_endpoint(&self) -> bool {
        matches!(
            self,
            Command::Metastore(_)
                | Command::Catalog(_)
                | Command::Schema(_)
                | Command::Table(_)
                | Command::Credentials(_)
        )
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use super::normalize;

    fn words(args: &[&str]) -> Vec<String> {
        normalize(args.iter().map(OsString::from))
            .into_iter()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect()
    }

    #[test]
    fn the_verb_first_spelling_becomes_the_noun_tree() {
        assert_eq!(
            words(&["pqbench", "get", "table", "X"]),
            ["pqbench", "table", "get", "X"]
        );
        assert_eq!(
            words(&["pqbench", "ls", "schema", "CAT"]),
            ["pqbench", "schema", "ls", "CAT"]
        );
    }

    #[test]
    fn the_noun_tree_is_untouched() {
        assert_eq!(
            words(&["pqbench", "table", "get", "X"]),
            ["pqbench", "table", "get", "X"]
        );
        assert_eq!(
            words(&["pqbench", "catalog", "ls"]),
            ["pqbench", "catalog", "ls"]
        );
    }

    #[test]
    fn a_verb_after_the_global_flags_is_rewritten() {
        assert_eq!(
            words(&["pqbench", "--endpoint", "https://x", "get", "catalog"]),
            ["pqbench", "--endpoint", "https://x", "catalog", "get"]
        );
        assert_eq!(
            words(&["pqbench", "--token=abc", "ls", "catalog"]),
            ["pqbench", "--token=abc", "catalog", "ls"]
        );
    }

    #[test]
    fn a_bare_verb_asks_for_help() {
        assert_eq!(words(&["pqbench", "get"]), ["pqbench", "--help"]);
        assert_eq!(
            words(&["pqbench", "ls", "--help"]),
            ["pqbench", "--help", "--help"]
        );
        assert_eq!(
            words(&["pqbench", "get", "typo"]),
            ["pqbench", "get", "typo"]
        );
    }
}
