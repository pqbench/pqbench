//! The walk's configuration: `~/.pqbenchrc` profiles, then the environment,
//! then the command line.
//!
//! `~/.pqbenchrc` is an INI of named profiles — the shape of `~/.databrickscfg`
//! — each naming an endpoint, a bearer, and a dialect, so one workspace can
//! carry a `[unity]` and an `[iceberg]` profile. Everything else is a
//! per-invocation flag. Least to most specific: the selected profile, then the
//! environment, then the flags.

use std::collections::BTreeMap;
use std::path::PathBuf;

use clap::Args;
use pqbench::config::{Combine, Dialect, Layer};

use crate::CliError;

/// The global connection flags every command shares, each with a `PQB_*`
/// fallback.
#[derive(Args)]
pub(crate) struct ConfigArgs {
    /// the `~/.pqbenchrc` profile to run under [env: PQB_PROFILE]
    #[arg(long, global = true, value_name = "NAME", env = "PQB_PROFILE")]
    pub profile: Option<String>,
    /// workspace URL; overrides the profile [env: PQB_ENDPOINT]
    #[arg(long, global = true, value_name = "URL", env = "PQB_ENDPOINT")]
    pub endpoint: Option<String>,
    /// bearer token; overrides the profile [env: PQB_TOKEN]
    #[arg(long, global = true, value_name = "TOKEN", env = "PQB_TOKEN")]
    pub token: Option<String>,
    /// catalog dialect: unity (the default) or iceberg [env: PQB_TABLE_FORMAT]
    #[arg(long, global = true, value_name = "FORMAT", env = "PQB_TABLE_FORMAT", value_parser = parse_dialect)]
    pub table_format: Option<Dialect>,
}

/// Resolve the run's configuration: the selected profile, then the environment,
/// then the command line.
///
/// # Errors
/// Fails when the profile is missing or unknown, a layer is malformed, or when
/// `require_endpoint` and no layer names an endpoint.
pub(crate) fn resolve(
    command: &str,
    args: &ConfigArgs,
    require_endpoint: bool,
) -> Result<pqbench::Config, CliError> {
    let config = profile(args)?
        .combine(env_layer())
        .combine(flag_layer(args))
        .resolve();
    if require_endpoint && config.endpoint.is_empty() {
        return Err(format!(
            "{command} needs an endpoint: --endpoint, PQB_ENDPOINT, DATABRICKS_HOST, or a ~/.pqbenchrc profile"
        )
        .into());
    }
    Ok(config)
}

/// The selected profile's contribution: `--profile`/`PQB_PROFILE`, else
/// `[DEFAULT]`, else the only profile there is.
fn profile(args: &ConfigArgs) -> Result<Layer, CliError> {
    let profiles = read_profiles()?;
    if profiles.is_empty() {
        return Ok(Layer::default());
    }
    match args.profile.as_deref() {
        Some(name) => profiles
            .get(name)
            .cloned()
            .ok_or_else(|| format!("no profile {name:?}; {}", names(&profiles)).into()),
        None => match profiles.get("DEFAULT") {
            Some(default) => Ok(default.clone()),
            None if profiles.len() == 1 => Ok(profiles.into_values().next().unwrap_or_default()),
            None => Err(format!("several profiles; pass --profile ({}).", names(&profiles)).into()),
        },
    }
}

/// Read `~/.pqbenchrc`'s profiles; a missing file is empty.
fn read_profiles() -> Result<BTreeMap<String, Layer>, CliError> {
    let Some(path) = config_path() else {
        return Ok(BTreeMap::new());
    };
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
        Err(error) => return Err(format!("cannot read {}: {error}", path.display()).into()),
    };
    parse_profiles(&text)
}

/// The config file's path: `PQB_CONFIG_FILE`, else `~/.pqbenchrc`.
fn config_path() -> Option<PathBuf> {
    std::env::var_os("PQB_CONFIG_FILE")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".pqbenchrc")))
}

/// One `[profile]` table per profile, each with `endpoint`, `token`, and
/// `table_format` keys, with `#` comments (INI).
fn parse_profiles(text: &str) -> Result<BTreeMap<String, Layer>, CliError> {
    let mut profiles: BTreeMap<String, Layer> = BTreeMap::new();
    let mut profile: Option<String> = None;
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }
        if let Some(name) = line
            .strip_prefix('[')
            .and_then(|line| line.strip_suffix(']'))
        {
            profile = Some(name.trim().to_string());
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let value = unquote(value.trim());
        if value.is_empty() {
            continue;
        }
        let Some(name) = profile.clone() else {
            continue;
        };
        let layer = profiles.entry(name).or_default();
        match key.trim() {
            "endpoint" => layer.endpoint = Some(value),
            "token" => layer.token = Some(value),
            "table_format" => {
                layer.dialect =
                    Some(parse_dialect(&value).map_err(|error| format!("~/.pqbenchrc: {error}"))?);
            }
            _ => {}
        }
    }
    Ok(profiles)
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
fn flag_layer(args: &ConfigArgs) -> Layer {
    Layer {
        endpoint: non_empty(args.endpoint.clone()),
        token: non_empty(args.token.clone()),
        dialect: args.table_format,
    }
}

/// Parse a dialect name: `unity` or `iceberg`.
fn parse_dialect(value: &str) -> Result<Dialect, CliError> {
    match value {
        "unity" => Ok(Dialect::Unity),
        "iceberg" => Ok(Dialect::Iceberg),
        other => Err(format!("table_format expects `unity` or `iceberg`, got {other:?}").into()),
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

/// The comma-separated profile names, for an error message.
fn names(profiles: &BTreeMap<String, Layer>) -> String {
    let names: Vec<&str> = profiles.keys().map(String::as_str).collect();
    format!("have {}", names.join(", "))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_run_control_file_reads_profiles_comments_and_keys() {
        let profiles = parse_profiles(
            "# pqbench's run-control file\n\
             [DEFAULT]\n\
             endpoint = https://example.cloud.databricks.com\n\
             token = dapi-secret\n\
             table_format = unity\n\
             \n\
             [iceberg]\n\
             endpoint = https://example.cloud.databricks.com\n\
             table_format = iceberg\n",
        )
        .unwrap();
        assert_eq!(
            profiles["DEFAULT"].endpoint.as_deref(),
            Some("https://example.cloud.databricks.com")
        );
        assert_eq!(profiles["DEFAULT"].dialect, Some(Dialect::Unity));
        assert_eq!(profiles["iceberg"].dialect, Some(Dialect::Iceberg));
        assert_eq!(profiles["iceberg"].token, None);
    }

    #[test]
    fn a_more_specific_layer_wins() {
        let profile = Layer {
            endpoint: Some("profile".into()),
            token: None,
            dialect: Some(Dialect::Iceberg),
        };
        let flags = Layer {
            endpoint: None,
            token: Some("flag".into()),
            dialect: None,
        };
        let config = profile.combine(flags).resolve();
        assert_eq!(config.endpoint, "profile");
        assert_eq!(config.token.as_deref(), Some("flag"));
        assert_eq!(config.dialect, Dialect::Iceberg);
    }
}
