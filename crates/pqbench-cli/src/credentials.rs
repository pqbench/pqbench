//! `pqbench credentials get`: vended storage options for a table.

use std::collections::BTreeMap;
use std::path::PathBuf;

use clap::{Args, Subcommand};
use futures_util::stream::StreamExt;
use pqbench::credentials::vend;
use serde::Serialize;
use serde_json::Value;

use crate::emit::{write_stdout, Align, Emitter, Format, Row};
use crate::source::{self, read_input, ref_env, table_ref};
use crate::CliError;

/// Arguments for `credentials`: one table's storage options.
#[derive(Args)]
pub(crate) struct CredentialsArgs {
    #[command(subcommand)]
    command: CredentialsCommand,
}

#[derive(Subcommand)]
pub(crate) enum CredentialsCommand {
    /// Put vended read credentials on each table-ref
    Get(GetArgs),
}

/// Arguments for `credentials get`: the output flags.
#[derive(Args)]
pub(crate) struct GetArgs {
    /// stdout format: auto (table on a terminal) | table | json
    #[arg(long, value_enum, default_value_t = Format::Auto)]
    format: Format,
    /// also write the lz4 NDJSON stream to FILE
    #[arg(short = 'o', long = "output", value_name = "FILE")]
    output: Option<PathBuf>,
    /// requests in flight at once
    #[arg(long, default_value_t = 64)]
    fan_out: usize,
    /// write the one ref's env as shell `export` lines for a loop's `eval`,
    /// not refs
    #[arg(long = "shell-env")]
    shell_env: bool,
}

pub(crate) async fn run(args: &CredentialsArgs) -> Result<(), CliError> {
    match &args.command {
        CredentialsCommand::Get(args) => run_get(args).await,
    }
}

/// Put vended read credentials on each ref: `schema ls | credentials get`.
///
/// `--shell-env` writes the one ref's env as shell assignments instead, so a
/// per-table loop can `eval` them into the process environment.
async fn run_get(args: &GetArgs) -> Result<(), CliError> {
    let input = read_input("credentials get").await?;
    if !input.piped {
        return Err("credentials get reads pqbench.table-ref v2 refs on standard input".into());
    }
    if args.shell_env {
        if args.output.is_some() || args.format != Format::Auto {
            return Err(
                "credentials get --shell-env writes shell assignments to stdout; drop --format / --output"
                    .into(),
            );
        }
        return export_credentials(input).await;
    }
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    let records = source::records("credentials get", input.first, input.lines);
    let source = input.source;
    let unity = matches!(source.table_format, source::TableFormat::Unity);
    let mut tables = 0;
    let mut vends = records
        .map(|record| async {
            let record = record?;
            let (catalog, schema, name) = table_ref("credentials get", &record)?;
            let credentials = if unity {
                vend(
                    &source.endpoint,
                    &catalog,
                    &schema,
                    &name,
                    source.token.as_deref(),
                )
                .await?
            } else {
                None
            };
            Ok::<_, CliError>((record, credentials))
        })
        .buffer_unordered(args.fan_out.max(1));
    while let Some(result) = vends.next().await {
        let (record, credentials) = result?;
        let vended = credentials.is_some();
        emit.write_row(&VendedRef {
            record: add_env(record, credentials, &source.env),
            vended,
        })
        .await?;
        tables += 1;
    }
    emit.finish(&format!("tables: {tables}\n")).await
}

/// `--shell-env`: one ref's env as shell assignments, for the loop's `eval`.
///
/// The env is the lake source's options, the ref's own, then the vended
/// credentials, so the loop can put them in the process environment once and
/// run the table's work under them.
async fn export_credentials(input: source::Input) -> Result<(), CliError> {
    let source = input.source;
    let mut records = source::records("credentials get", input.first, input.lines);
    let Some(record) = records.next().await else {
        return Err("credentials get --shell-env takes one pqbench.table-ref".into());
    };
    let record = record?;
    if records.next().await.transpose()?.is_some() {
        return Err("credentials get --shell-env populates one table's env; feed one ref".into());
    }
    let (catalog, schema, name) = table_ref("credentials get", &record)?;
    let credentials = if matches!(source.table_format, source::TableFormat::Unity) {
        vend(
            &source.endpoint,
            &catalog,
            &schema,
            &name,
            source.token.as_deref(),
        )
        .await?
    } else {
        None
    };
    let env = table_env(&record, credentials, &source.env);
    write_stdout(&export_lines(&env)).await
}

/// The table's env: the lake source's options, the ref's own, then the vended
/// credentials. The vended keys win, so a table-scoped lease replaces any
/// static key for that table.
fn table_env(
    record: &Value,
    credentials: Option<BTreeMap<String, String>>,
    source: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let mut env = ref_env(record, source);
    if let Some(credentials) = credentials {
        env.extend(credentials);
    }
    env
}

/// The env as shell `export` lines, one variable per line; the value is single
/// quoted, so `eval` sets it literally (`'` becomes `'\''`).
fn export_lines(env: &BTreeMap<String, String>) -> String {
    let mut lines = String::new();
    for (key, value) in env {
        lines.push_str(&format!(
            "export {key}='{}'\n",
            value.replace('\'', "'\\''")
        ));
    }
    lines
}

/// The ref's `env` with the vended credentials written over it.
fn add_env(
    mut record: Value,
    credentials: Option<BTreeMap<String, String>>,
    source: &BTreeMap<String, String>,
) -> Value {
    let Some(credentials) = credentials else {
        return record;
    };
    let env = table_env(&record, Some(credentials), source);
    let Some(object) = record.as_object_mut() else {
        return record;
    };
    let mut values = serde_json::Map::new();
    for (key, value) in env {
        values.insert(key, Value::String(value));
    }
    object.insert("env".to_string(), Value::Object(values));
    record
}

/// The row `credentials get` writes: the ref, plus whether creds were vended.
/// The serialized form is the ref itself.
struct VendedRef {
    record: Value,
    vended: bool,
}

impl Serialize for VendedRef {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.record.serialize(serializer)
    }
}

impl Row for VendedRef {
    const HEADER: &'static [&'static str] = &["name", "credentials"];
    const ALIGN: &[Align] = &[Align::Left, Align::Left];

    fn cells(&self) -> Vec<String> {
        vec![
            self.record["id"].as_str().unwrap_or_default().to_string(),
            if self.vended { "vended" } else { "-" }.to_string(),
        ]
    }
}
