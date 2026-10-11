//! The walk's configuration: the layers that resolve it, least to most
//! specific — `~/.pqbenchrc`, then the environment, then the command line.
//!
//! `~/.pqbenchrc` has a shared top-level table (endpoint, token, table_format)
//! and per-command `[section]` tables (`[table]`, `[ratelimit]`, …) that only
//! the command they name reads — the same split uv gives `[tool.uv.pip]`. Every
//! setting is also a flag with a `PQB_*` fallback; the file is the lowest layer.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::str::FromStr;

use clap::Args;
use pqbench::config::{Combine, Dialect, Layer};

use crate::CliError;

/// The configuration flags every command shares: parsed from the command line
/// with a `PQB_*` environment fallback (the `env` attribute).
#[derive(Args)]
pub(crate) struct ConfigArgs {
    /// workspace URL [env: PQB_ENDPOINT]
    #[arg(long, global = true, value_name = "URL", env = "PQB_ENDPOINT")]
    pub endpoint: Option<String>,
    /// bearer token [env: PQB_TOKEN]
    #[arg(long, global = true, value_name = "TOKEN", env = "PQB_TOKEN")]
    pub token: Option<String>,
    /// catalog dialect: unity (the default) or iceberg [env: PQB_TABLE_FORMAT]
    #[arg(long, global = true, value_name = "FORMAT", env = "PQB_TABLE_FORMAT")]
    pub table_format: Option<String>,
    /// override a setting for this run: NAME=VALUE, or SECTION.KEY=VALUE
    #[arg(long = "set", global = true, value_name = "NAME=VALUE")]
    pub set: Vec<String>,
}

/// The resolved configuration and the run-control file's per-command sections.
pub(crate) struct Resolution {
    /// The shared connection config, ready for [`pqbench::Context`].
    pub config: pqbench::Config,
    /// The `[section]` tables, read by the command they name.
    pub sections: Sections,
}

/// Resolve the run's configuration: the run-control file, then the environment,
/// then the command line.
///
/// # Errors
/// Fails when a layer is malformed, or when `require_endpoint` and no layer
/// names an endpoint.
pub(crate) fn resolve(
    command: &str,
    args: &ConfigArgs,
    require_endpoint: bool,
) -> Result<Resolution, CliError> {
    let run_control = read_run_control()?;
    let set = parse_set(&args.set)?;
    let config = run_control
        .shared
        .combine(env_layer())
        .combine(flag_layer(command, args)?)
        .combine(set.shared)
        .resolve();
    if require_endpoint && config.endpoint.is_empty() {
        return Err(format!(
            "{command} needs an endpoint: --endpoint, PQB_ENDPOINT, DATABRICKS_HOST, or ~/.pqbenchrc"
        )
        .into());
    }
    Ok(Resolution {
        config,
        sections: run_control.sections.overlay(set.sections),
    })
}

/// The run-control file, parsed: the shared top-level table and the per-command
/// `[section]` tables.
#[derive(Default)]
struct RunControl {
    shared: Layer,
    sections: Sections,
}

/// The run-control file's `[section]` tables, keyed by section and then key.
#[derive(Default)]
pub(crate) struct Sections(BTreeMap<String, BTreeMap<String, String>>);

impl Sections {
    /// One value, or `None` when the section or key is absent.
    pub(crate) fn get(&self, section: &str, key: &str) -> Option<&str> {
        self.0.get(section)?.get(key).map(String::as_str)
    }

    /// One value parsed as `T`, naming the source in an error.
    ///
    /// # Errors
    /// Fails when the value cannot be parsed as `T`.
    pub(crate) fn parse<T: FromStr>(&self, section: &str, key: &str) -> Result<Option<T>, CliError>
    where
        <T as FromStr>::Err: std::fmt::Display,
    {
        match self.get(section, key) {
            Some(value) => value
                .parse::<T>()
                .map(Some)
                .map_err(|error| format!("[{section}] {key}: {error}").into()),
            None => Ok(None),
        }
    }

    /// Layer `higher` over `self`: a section/key present in `higher` wins.
    fn overlay(&self, higher: Sections) -> Sections {
        let mut merged = self.0.clone();
        for (section, keys) in higher.0 {
            merged.entry(section).or_default().extend(keys);
        }
        Sections(merged)
    }
}

/// Parse `--set` overrides into a top layer: `SECTION.KEY=VALUE` sets a section
/// key, a bare `endpoint`/`token`/`table_format` sets the shared table.
fn parse_set(values: &[String]) -> Result<RunControl, CliError> {
    let mut run_control = RunControl::default();
    for value in values {
        let Some((name, set)) = value.split_once('=') else {
            return Err(format!("--set expects NAME=VALUE, got {value:?}").into());
        };
        match name.rsplit_once('.') {
            Some((section, key)) => {
                run_control
                    .sections
                    .0
                    .entry(section.to_string())
                    .or_default()
                    .insert(key.to_string(), set.to_string());
            }
            None => match name {
                "endpoint" => run_control.shared.endpoint = Some(set.to_string()),
                "token" => run_control.shared.token = Some(set.to_string()),
                "table_format" => {
                    run_control.shared.dialect = Some(parse_dialect("--set", set)?);
                }
                other => return Err(format!("--set: unknown setting {other:?}").into()),
            },
        }
    }
    Ok(run_control)
}

/// The run-control file's contribution: the shared table and the sections.
fn read_run_control() -> Result<RunControl, CliError> {
    let Some(path) = run_control_path() else {
        return Ok(RunControl::default());
    };
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(RunControl::default())
        }
        Err(error) => return Err(format!("cannot read {}: {error}", path.display()).into()),
    };
    parse_run_control(&text)
}

/// The run-control file's path: `PQB_CONFIG_FILE`, else `~/.pqbenchrc`.
fn run_control_path() -> Option<PathBuf> {
    std::env::var_os("PQB_CONFIG_FILE")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".pqbenchrc")))
}

/// The shared `key = value` lines, then one `[section]` table per command, with
/// `#` comments (taoup's run-control syntax).
fn parse_run_control(text: &str) -> Result<RunControl, CliError> {
    let mut run_control = RunControl::default();
    let mut section: Option<String> = None;
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(name) = line
            .strip_prefix('[')
            .and_then(|line| line.strip_suffix(']'))
        {
            section = Some(name.trim().to_string());
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let value = unquote(value.trim());
        if value.is_empty() {
            continue;
        }
        match &section {
            None => match key.trim() {
                "endpoint" => run_control.shared.endpoint = Some(value),
                "token" => run_control.shared.token = Some(value),
                "table_format" => {
                    run_control.shared.dialect = Some(parse_dialect("~/.pqbenchrc", &value)?);
                }
                _ => {}
            },
            Some(name) => {
                run_control
                    .sections
                    .0
                    .entry(name.clone())
                    .or_default()
                    .insert(key.trim().to_string(), value);
            }
        }
    }
    Ok(run_control)
}

/// The environment's contribution. clap already merged the flags over `PQB_*`,
/// so this layer is the lower-priority `DATABRICKS_*` fallback.
fn env_layer() -> Layer {
    Layer {
        endpoint: env_var("DATABRICKS_HOST"),
        token: env_var("DATABRICKS_TOKEN"),
        dialect: None,
    }
}

/// The command line's contribution, with clap's `PQB_*` fallback already
/// applied.
fn flag_layer(command: &str, args: &ConfigArgs) -> Result<Layer, CliError> {
    let dialect = match args.table_format.as_deref() {
        Some(format) => Some(parse_dialect(command, format)?),
        None => None,
    };
    Ok(Layer {
        endpoint: non_empty(args.endpoint.clone()),
        token: non_empty(args.token.clone()),
        dialect,
    })
}

/// Parse a dialect name (`unity` or `iceberg`), naming `source` in the error.
fn parse_dialect(source: &str, value: &str) -> Result<Dialect, CliError> {
    match value {
        "unity" => Ok(Dialect::Unity),
        "iceberg" => Ok(Dialect::Iceberg),
        other => Err(
            format!("{source}: table_format expects `unity` or `iceberg`, got {other:?}").into(),
        ),
    }
}

/// A value with matching surrounding quotes removed.
fn unquote(value: &str) -> String {
    let bytes = value.as_bytes();
    let quoted = bytes.len() >= 2
        && (bytes[0] == b'"' || bytes[0] == b'\'')
        && bytes[bytes.len() - 1] == bytes[0];
    if quoted {
        value[1..value.len() - 1].to_string()
    } else {
        value.to_string()
    }
}

fn env_var(name: &str) -> Option<String> {
    non_empty(std::env::var(name).ok())
}

fn non_empty(value: Option<String>) -> Option<String> {
    value.filter(|value| !value.trim().is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_run_control_file_reads_keys_comments_and_sections() {
        let run_control = parse_run_control(
            "# pqbench's run-control file\n\
             endpoint = https://example.cloud.databricks.com\n\
             table_format=iceberg\n\
             \n\
             [scan]\n\
             fan_out = 8\n\
             \n\
             nonsense\n",
        )
        .unwrap();
        assert_eq!(
            run_control.shared.endpoint.as_deref(),
            Some("https://example.cloud.databricks.com")
        );
        assert_eq!(run_control.shared.dialect, Some(Dialect::Iceberg));
        assert_eq!(
            run_control
                .sections
                .parse::<usize>("scan", "fan_out")
                .unwrap(),
            Some(8)
        );
        assert_eq!(run_control.sections.get("scan", "missing"), None);
    }

    #[test]
    fn set_overrides_the_file() {
        let set = parse_set(&[
            "scan.fan_out=4".to_string(),
            "table_format=unity".to_string(),
        ])
        .unwrap();
        let file = RunControl {
            shared: Layer {
                dialect: Some(Dialect::Iceberg),
                ..Layer::default()
            },
            sections: Sections(
                [(
                    "scan".to_string(),
                    [("fan_out".to_string(), "8".to_string())]
                        .into_iter()
                        .collect(),
                )]
                .into_iter()
                .collect(),
            ),
        };
        let sections = file.sections.overlay(set.sections);
        assert_eq!(sections.get("scan", "fan_out"), Some("4"));
        assert_eq!(set.shared.dialect, Some(Dialect::Unity));
    }

    #[test]
    fn a_more_specific_layer_wins() {
        let file = Layer {
            endpoint: Some("file".into()),
            token: None,
            dialect: Some(Dialect::Iceberg),
        };
        let flags = Layer {
            endpoint: None,
            token: Some("flag".into()),
            dialect: None,
        };
        let config = file.combine(flags).resolve();
        assert_eq!(config.endpoint, "file");
        assert_eq!(config.token.as_deref(), Some("flag"));
        assert_eq!(config.dialect, Dialect::Iceberg);
    }
}
