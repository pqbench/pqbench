//! The walk's input: the environment's config and the parent-level refs.
//!
//! Every level is a stateless filter: it reads only its parent's records
//! (`list schemas` reads `Catalog` names, one per line) and writes its own. The
//! endpoint, bearer, and dialect come from the resolved configuration —
//! `~/.pqbenchrc`, the environment, then the flags (`pqbench setup` prints it
//! for the shell to eval):
//!
//! ```text
//! PQB_ENDPOINT=… PQB_TOKEN=… pqbench list catalogs | pqbench list schemas | …
//! ```
//!
//! `tee` writes each level into the job tree; `get` stages emit the same
//! resource as the `list` they read from, so they enrich a stream in place.
//!
//! Records are read one at a time as the command consumes them, so a level
//! never buffers its parent: a slow consumer stops reading, and the pipe
//! backpressures the producer.

use futures_util::stream::{self, Stream, StreamExt};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, BufReader, Lines};

use crate::stdin::is_stream;
use crate::CliError;

/// The parent records on standard input: whether standard input is a stream,
/// the peeked first record, and the rest of the lines.
pub(crate) struct Input {
    /// Whether standard input is a stream (a pipe or a redirected file), not a
    /// terminal or other descriptor a read would block on.
    pub piped: bool,
    /// The first record, when stdin carried one.
    pub first: Option<Value>,
    /// The lines after the first record.
    pub lines: Lines<BufReader<tokio::io::Stdin>>,
}

/// The parent records: the peeked first record, then the rest of stdin.
pub(crate) fn records(
    command: &'static str,
    first: Option<Value>,
    lines: Lines<BufReader<tokio::io::Stdin>>,
) -> impl Stream<Item = Result<Value, CliError>> + Unpin {
    let first = stream::iter(first).map(Ok::<_, CliError>);
    let rest = stream::try_unfold(lines, move |mut lines| async move {
        loop {
            let Some(line) = lines.next_line().await? else {
                return Ok(None);
            };
            if line.trim().is_empty() {
                continue;
            }
            let record: Value = serde_json::from_str(&line)
                .map_err(|error| format!("{command} reads NDJSON records: {error}"))?;
            return Ok(Some((record, lines)));
        }
    });
    Box::pin(first.chain(rest))
}

/// Read stdin's first record (a parent ref, kept for the command's stream),
/// without resolving config.
pub(crate) async fn read_input(command: &'static str) -> Result<Input, CliError> {
    let piped = is_stream();
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut first = None;
    if piped {
        while let Some(line) = lines.next_line().await? {
            if line.trim().is_empty() {
                continue;
            }
            let record: Value = serde_json::from_str(&line)
                .map_err(|error| format!("{command} reads NDJSON records: {error}"))?;
            first = Some(record);
            break;
        }
    }
    Ok(Input {
        piped,
        first,
        lines,
    })
}

/// Split `catalog.schema` at the first dot; an Iceberg namespace keeps its
/// remaining dots.
pub(crate) fn split_schema(command: &str, fqn: &str) -> Result<(String, String), CliError> {
    let Some((catalog, schema)) = fqn.split_once('.') else {
        return Err(format!("{command} takes CATALOG.SCHEMA; got {fqn:?}").into());
    };
    if catalog.is_empty() || schema.is_empty() {
        return Err(format!("{command} takes CATALOG.SCHEMA; got {fqn:?}").into());
    }
    Ok((catalog.to_string(), schema.to_string()))
}

/// The dotted `catalog.schema.table` a table ref names, for a message; the ref
/// itself when the name is not a table ref (a path read names a local table).
pub(crate) fn dotted_name(name: &str) -> String {
    match pqbench::v1::name::parse_table(name) {
        Some((catalog, schema, table)) => format!("{catalog}.{schema}.{table}"),
        None => name.to_string(),
    }
}

/// The storage location a table ref names: the [`Table`]'s `uri`. `None` when
/// the ref names no location (a view, or an Iceberg identifier until
/// `get table` resolves it).
///
/// [`Table`]: pqbench::v1::Table
pub(crate) fn ref_storage(record: &Value) -> Option<&str> {
    record["uri"].as_str().filter(|value| !value.is_empty())
}

/// Split `catalog.schema.table` at the first and last dots; an Iceberg
/// namespace keeps its remaining dots.
pub(crate) fn split_table(command: &str, fqn: &str) -> Result<(String, String, String), CliError> {
    let Some((catalog, rest)) = fqn.split_once('.') else {
        return Err(format!("{command} takes CATALOG.SCHEMA.TABLE; got {fqn:?}").into());
    };
    let Some((schema, table)) = rest.rsplit_once('.') else {
        return Err(format!("{command} takes CATALOG.SCHEMA.TABLE; got {fqn:?}").into());
    };
    if catalog.is_empty() || schema.is_empty() || table.is_empty() {
        return Err(format!("{command} takes CATALOG.SCHEMA.TABLE; got {fqn:?}").into());
    }
    Ok((catalog.to_string(), schema.to_string(), table.to_string()))
}

/// The vended lease a ref carries, when it carries one.
pub(crate) fn ref_credentials(record: &Value) -> Option<pqbench::v1::Credentials> {
    serde_json::from_value(record["credentials"].clone()).ok()
}

/// What a level's parent stream carries (and what its one argument names).
#[derive(Clone, Copy)]
pub(crate) enum Carried {
    Metastore,
    Catalog,
    Schema,
    Table,
}

impl Carried {
    /// The positional's spelling, for messages.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Metastore => "METASTORE",
            Self::Catalog => "CATALOG",
            Self::Schema => "CATALOG.SCHEMA",
            Self::Table => "CATALOG.SCHEMA.TABLE",
        }
    }

    /// The stream's noun, for messages.
    pub(crate) fn noun(self) -> &'static str {
        match self {
            Self::Metastore => "catalog",
            Self::Catalog => "catalog",
            Self::Schema => "schema",
            Self::Table => "table",
        }
    }

    /// The resource name a parent record carries.
    pub(crate) fn record_name(self, record: &Value) -> Result<String, CliError> {
        let name = record["name"].as_str().unwrap_or_default();
        let well_formed = match self {
            Self::Metastore => pqbench::v1::name::parse_metastore(name).is_some(),
            Self::Catalog => pqbench::v1::name::parse_catalog(name).is_some(),
            Self::Schema => pqbench::v1::name::parse_schema(name).is_some(),
            Self::Table => pqbench::v1::name::parse_table(name).is_some(),
        };
        if well_formed {
            return Ok(name.to_owned());
        }
        Err(format!(
            "a {} record needs a `{}` name",
            self.noun(),
            match self {
                Self::Metastore => "metastores/…",
                Self::Catalog => "catalogs/…",
                Self::Schema => "catalogs/…/schemas/…",
                Self::Table => "catalogs/…/tables/…",
            }
        )
        .into())
    }

    /// The resource name the positional spells.
    pub(crate) fn argument_name(self, command: &str, argument: &str) -> Result<String, CliError> {
        match self {
            Self::Catalog if !argument.is_empty() => Ok(pqbench::v1::name::catalog(argument)),
            Self::Schema => {
                let (catalog, schema) = split_schema(command, argument)?;
                Ok(pqbench::v1::name::schema(&catalog, &schema))
            }
            Self::Table => {
                let (catalog, schema, table) = split_table(command, argument)?;
                Ok(pqbench::v1::name::table(&catalog, &schema, &table))
            }
            _ => Err(format!("{command} takes no {} argument", self.label()).into()),
        }
    }
}

/// The level's parents: its one positional, or the parent stream on standard
/// input — exactly one. A positional becomes the record its name stands for,
/// so the read below sees one shape either way.
pub(crate) async fn parents(
    command: &'static str,
    argument: Option<&str>,
    carried: Carried,
) -> Result<impl Stream<Item = Result<Value, CliError>> + Unpin, CliError> {
    let input = read_input(command).await?;
    let present = input.first.is_some();
    match (argument, present, input.piped) {
        (Some(_), true, _) => Err(format!(
            "{command} takes {} or a {} stream, not both",
            carried.label(),
            carried.noun()
        )
        .into()),
        (Some(argument), false, _) => {
            let name = carried.argument_name(command, argument)?;
            Ok(stream::iter([Ok(serde_json::json!({ "name": name }))]).boxed())
        }
        (None, _, false) => Err(format!(
            "{command} needs {} or a {} stream",
            carried.label(),
            carried.noun()
        )
        .into()),
        (None, _, true) => Ok(records(command, input.first, input.lines)
            .map(move |record| {
                record.and_then(|record| carried.record_name(&record).map(|_| record))
            })
            .boxed()),
    }
}

/// [`parents`], as the resource names alone.
pub(crate) async fn parent_names(
    command: &'static str,
    argument: Option<&str>,
    carried: Carried,
) -> Result<impl Stream<Item = Result<String, CliError>> + Unpin, CliError> {
    Ok(parents(command, argument, carried)
        .await?
        .map(|record| record.map(|record| record["name"].as_str().unwrap_or_default().to_owned())))
}

/// Write every record a level's parent expands to, reporting what fails and
/// counting it.
///
/// This is fs2's `parEvalMapUnordered(n)(read) >>> flatMap(Stream.emits) >>>
/// compile.drain` with the failure branch kept: one parent becomes many records,
/// each read may fail, and a failure — of the read or of one record — is named
/// on standard error while the stream carries on. Returns the records lost.
pub(crate) async fn expand<I, T, F>(
    items: impl Stream<Item = Result<I, CliError>>,
    fan_out: usize,
    emit: &mut crate::emit::Emitter,
    read: F,
) -> Result<usize, CliError>
where
    T: serde::Serialize,
    F: std::ops::AsyncFn(I) -> Result<Vec<Result<T, CliError>>, CliError> + Copy,
{
    let reads = items
        .map(|item| async move { read(item?).await })
        .buffer_unordered(fan_out.max(1));
    futures_util::pin_mut!(reads);
    let mut dropped = 0;
    while let Some(read) = reads.next().await {
        let rows = match read {
            Ok(rows) => rows,
            Err(error) => {
                crate::error::report(&error);
                dropped += 1;
                continue;
            }
        };
        for row in rows {
            match row {
                Ok(row) => emit.write(&row).await?,
                Err(error) => {
                    crate::error::report(&error);
                    dropped += 1;
                }
            }
        }
    }
    Ok(dropped)
}

/// [`expand`] for the levels whose parent yields records that cannot fail one
/// by one: one read, many rows, all of them kept.
pub(crate) async fn stage<I, T, F>(
    items: impl Stream<Item = Result<I, CliError>>,
    fan_out: usize,
    emit: &mut crate::emit::Emitter,
    read: F,
) -> Result<usize, CliError>
where
    T: serde::Serialize,
    F: std::ops::AsyncFn(I) -> Result<Vec<T>, CliError> + Copy,
{
    expand(items, fan_out, emit, |item| async move {
        read(item)
            .await
            .map(|rows| rows.into_iter().map(Ok::<_, CliError>).collect())
    })
    .await
}
