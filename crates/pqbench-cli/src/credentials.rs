//! `pqbench credentials get`: vended storage options for a table.

use std::collections::BTreeMap;
use std::path::PathBuf;

use clap::{Args, Subcommand};
use futures_util::stream::StreamExt;
use serde::Serialize;
use serde_json::Value;

use crate::emit::{Align, Emitter, Format, Row};
use crate::source::{self, read_input, ref_env, table_ref, vend};
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
    /// Check each table-ref is readable outside Databricks compute
    Check(CheckArgs),
}

/// Arguments for `credentials get`: the shared output flags.
#[derive(Args)]
pub(crate) struct GetArgs {
    #[command(flatten)]
    stage: StageArgs,
}

/// Arguments for `credentials check`: the shared output flags.
#[derive(Args)]
pub(crate) struct CheckArgs {
    #[command(flatten)]
    stage: StageArgs,
}

/// The output flags both credentials stages share.
#[derive(Args)]
pub(crate) struct StageArgs {
    /// stdout format: auto (table on a terminal) | table | json
    #[arg(long, value_enum, default_value_t = Format::Auto)]
    format: Format,
    /// also write the lz4 NDJSON stream to FILE
    #[arg(short = 'o', long = "output", value_name = "FILE")]
    output: Option<PathBuf>,
    /// requests in flight at once
    #[arg(long, default_value_t = 64)]
    fan_out: usize,
}

pub(crate) async fn run(args: &CredentialsArgs) -> Result<(), CliError> {
    match &args.command {
        CredentialsCommand::Get(args) => run_get(args).await,
        CredentialsCommand::Check(args) => run_check(args).await,
    }
}

/// Put vended read credentials on each ref: `schema ls | credentials get`.
async fn run_get(args: &GetArgs) -> Result<(), CliError> {
    let input = read_input("credentials get").await?;
    if !input.piped {
        return Err("credentials get reads pqbench.table-ref v2 refs on standard input".into());
    }
    let mut emit = Emitter::open(
        args.stage.output.as_deref(),
        args.stage.format.resolve(false),
    )?;
    let records = source::records("credentials get", input.first, input.lines);
    let source = input.source;
    let mut tables = 0;
    let mut vends = records
        .map(|record| async {
            let record = record?;
            let (catalog, schema, name) = table_ref("credentials get", &record)?;
            let credentials = vend(&source, &catalog, &schema, &name).await?;
            Ok::<_, CliError>((record, credentials))
        })
        .buffer_unordered(args.stage.fan_out.max(1));
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

/// Filter each ref's table on the catalog's eligibility:
/// `schema ls | credentials check | credentials get`.
///
/// The capability manifest names the tables only Databricks compute reads
/// (managed default storage, a view). Those have no external read at all, so
/// the check writes the reason to standard error and drops them instead of
/// letting a later storage read fail on credentials. Eligible refs pass
/// through unchanged, so the stage composes ahead of `credentials get` and a
/// mixed schema keeps going.
async fn run_check(args: &CheckArgs) -> Result<(), CliError> {
    let input = read_input("credentials check").await?;
    if !input.piped {
        return Err("credentials check reads pqbench.table-ref v2 refs on standard input".into());
    }
    let mut emit = Emitter::open(
        args.stage.output.as_deref(),
        args.stage.format.resolve(false),
    )?;
    let records = source::records("credentials check", input.first, input.lines);
    let source = input.source;
    let mut tables = 0;
    let mut dropped = 0;
    let mut checks = records
        .map(|record| async {
            let record = record?;
            let (catalog, schema, name) = table_ref("credentials check", &record)?;
            let reason = check(&source, &catalog, &schema, &name).await?;
            Ok::<_, CliError>((record, reason))
        })
        .buffer_unordered(args.stage.fan_out.max(1));
    while let Some(checked) = checks.next().await {
        let (record, reason) = checked?;
        if let Some(reason) = reason {
            eprintln!("error: {reason}");
            dropped += 1;
            continue;
        }
        emit.write_row(&CheckedRef { record }).await?;
        tables += 1;
    }
    emit.finish(&format!("tables: {tables}\n")).await?;
    if dropped > 0 {
        return Err(
            format!("{dropped} table(s) are not readable outside Databricks compute").into(),
        );
    }
    Ok(())
}

/// The reason a table is not readable outside Databricks compute, `None` when
/// it is. Iceberg reads its metadata inline through the catalog, so the
/// eligibility gate is Unity's alone.
async fn check(
    source: &source::Source,
    catalog: &str,
    schema: &str,
    name: &str,
) -> Result<Option<String>, CliError> {
    if !matches!(source.table_format, source::TableFormat::Unity) {
        return Ok(None);
    }
    let eligibility = pqbench::credentials::check::check_unity(
        &source.endpoint,
        catalog,
        schema,
        name,
        source.token.as_deref(),
    )
    .await?;
    if eligibility == pqbench::credentials::check::Eligibility::Ineligible {
        return Ok(Some(ineligible(&format!("{catalog}.{schema}.{name}"))));
    }
    Ok(None)
}

/// The message an ineligible table reports.
fn ineligible(name: &str) -> String {
    format!(
        "{name} is not readable outside Databricks compute: the catalog reports no direct external \
         engine read support (managed default storage or a view); read it with Databricks compute, \
         or copy it to an external location"
    )
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

/// The row `credentials check` writes: the ref, gated on eligibility. The
/// serialized form is the ref itself.
struct CheckedRef {
    record: Value,
}

impl Serialize for CheckedRef {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.record.serialize(serializer)
    }
}

impl Row for CheckedRef {
    const HEADER: &'static [&'static str] = &["name", "eligible"];
    const ALIGN: &[Align] = &[Align::Left, Align::Left];

    fn cells(&self) -> Vec<String> {
        vec![
            self.record["id"].as_str().unwrap_or_default().to_string(),
            "yes".to_string(),
        ]
    }
}
