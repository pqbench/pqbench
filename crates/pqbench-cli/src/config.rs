//! The walk's configuration: the layers that resolve it, least to most
//! specific — `~/.pqbenchrc`, then the environment, then the command line.
//!
//! `pqbench setup` prints the resolved values for the shell to `eval`, and every
//! command reads the same resolution through its [`pqbench::Context`].

use std::path::PathBuf;

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
) -> Result<pqbench::Config, CliError> {
    let config = read_run_control()?
        .combine(env_layer())
        .combine(flag_layer(command, args)?)
        .resolve();
    if require_endpoint && config.endpoint.is_empty() {
        return Err(format!(
            "{command} needs an endpoint: --endpoint, PQB_ENDPOINT, DATABRICKS_HOST, or ~/.pqbenchrc"
        )
        .into());
    }
    Ok(config)
}

/// The run-control file's contribution: durable, non-secret preferences.
fn read_run_control() -> Result<Layer, CliError> {
    let Some(path) = run_control_path() else {
        return Ok(Layer::default());
    };
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Layer::default()),
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

/// One `key = value` line per preference, with `#` comments (taoup's run-control
/// syntax): `endpoint`, `token`, and `table_format`.
fn parse_run_control(text: &str) -> Result<Layer, CliError> {
    let mut layer = Layer::default();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let value = unquote(value.trim());
        if value.is_empty() {
            continue;
        }
        match key.trim() {
            "endpoint" => layer.endpoint = Some(value),
            "token" => layer.token = Some(value),
            "table_format" => layer.dialect = Some(parse_dialect("~/.pqbenchrc", &value)?),
            _ => {}
        }
    }
    Ok(layer)
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
    fn the_run_control_file_reads_keys_and_comments() {
        let layer = parse_run_control(
            "# pqbench's run-control file\n\
             endpoint = https://example.cloud.databricks.com\n\
             table_format=iceberg\n\
             region = \"us-east-1\"\n\
             \n\
             nonsense\n",
        )
        .unwrap();
        assert_eq!(
            layer.endpoint.as_deref(),
            Some("https://example.cloud.databricks.com")
        );
        assert_eq!(layer.dialect, Some(Dialect::Iceberg));
        assert_eq!(layer.token, None);
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
