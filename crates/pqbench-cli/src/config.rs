//! The walk's configuration: `~/.pqbenchrc` profiles, then the environment,
//! then the command line.
//!
//! `~/.pqbenchrc` is an INI of named profiles — the shape of `~/.databrickscfg`
//! — each naming an endpoint, a bearer, and a dialect, so one workspace can
//! carry a `[unity]` and an `[iceberg]` profile. A profile may instead name a
//! service principal's client id and secret, and the bearer is minted from them
//! (`pqbench::oauth`). Everything else is a per-invocation flag. Least to most
//! specific: the selected profile, then the environment, then the flags.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use clap::Args;
use pqbench::config::{Combine, Dialect, Layer};
use pqbench::Secret;

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
    /// service-principal OAuth client id; mints the bearer when no token is set
    /// [env: PQB_CLIENT_ID]
    #[arg(
        long = "client-id",
        global = true,
        value_name = "ID",
        env = "PQB_CLIENT_ID"
    )]
    pub client_id: Option<String>,
    /// service-principal OAuth client secret [env: PQB_CLIENT_SECRET]
    #[arg(
        long = "client-secret",
        global = true,
        value_name = "SECRET",
        env = "PQB_CLIENT_SECRET"
    )]
    pub client_secret: Option<String>,
    /// catalog dialect: unity (the default) or iceberg [env: PQB_TABLE_FORMAT]
    #[arg(long, global = true, value_name = "FORMAT", env = "PQB_TABLE_FORMAT", value_parser = parse_dialect)]
    pub table_format: Option<Dialect>,
}

/// How long one request may take. The library leaves the bound to its caller;
/// ten seconds covers a cross-region 256 KiB S3 footer GET under concurrent
/// fan-out while still failing fast on a hung socket so the retry loop can
/// recover it.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

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
    let mut config = profile(args)?
        .combine(env_layer())
        .combine(flag_layer(args))
        .resolve();
    config.request_timeout = Some(REQUEST_TIMEOUT);
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

/// The config file's path: `PQB_CONFIG_FILE`, else `PQB_CONFIG`, else
/// `~/.pqbenchrc`. The driver reads the same file the same way.
fn config_path() -> Option<PathBuf> {
    ["PQB_CONFIG_FILE", "PQB_CONFIG"]
        .iter()
        .find_map(|name| std::env::var_os(name).map(PathBuf::from))
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".pqbenchrc")))
}

/// One `[profile]` table per profile, each with `endpoint`, `token`,
/// `table_format`, and the service principal's `client-id`/`client-secret`
/// keys, with `#` comments (INI). A key's hyphen and underscore spellings are
/// the same key, as they are in the driver's run-control reader.
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
        match key.trim().replace('_', "-").as_str() {
            "endpoint" => layer.endpoint = Some(value),
            "token" => layer.token = Some(value),
            "client-id" => layer.client_id = Some(value),
            "client-secret" => layer.client_secret = Some(Secret::new(value)),
            "table-format" => {
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
        client_id: env_var("DATABRICKS_CLIENT_ID"),
        client_secret: env_var("DATABRICKS_CLIENT_SECRET").map(Secret::new),
    }
}

/// The command line's contribution, with clap's `PQB_*` fallback already
/// applied.
fn flag_layer(args: &ConfigArgs) -> Layer {
    Layer {
        endpoint: non_empty(args.endpoint.clone()),
        token: non_empty(args.token.clone()),
        dialect: args.table_format,
        client_id: non_empty(args.client_id.clone()),
        client_secret: non_empty(args.client_secret.clone()).map(Secret::new),
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
             table_format = iceberg\n\
             \n\
             [principal]\n\
             endpoint = https://example.cloud.databricks.com\n\
             client_id = 0123-4567\n\
             client-secret = oauth-secret\n",
        )
        .unwrap();
        assert_eq!(
            profiles["DEFAULT"].endpoint.as_deref(),
            Some("https://example.cloud.databricks.com")
        );
        assert_eq!(profiles["DEFAULT"].dialect, Some(Dialect::Unity));
        assert_eq!(profiles["iceberg"].dialect, Some(Dialect::Iceberg));
        assert_eq!(profiles["iceberg"].token, None);
        assert_eq!(profiles["principal"].token, None);
        assert_eq!(
            profiles["principal"].client_id.as_deref(),
            Some("0123-4567")
        );
        assert_eq!(
            profiles["principal"]
                .client_secret
                .as_ref()
                .map(Secret::expose),
            Some("oauth-secret")
        );
    }

    #[test]
    fn a_more_specific_layer_wins() {
        let profile = Layer {
            endpoint: Some("profile".into()),
            token: None,
            dialect: Some(Dialect::Iceberg),
            ..Layer::default()
        };
        let flags = Layer {
            token: Some("flag".into()),
            ..Layer::default()
        };
        let config = profile.combine(flags).resolve();
        assert_eq!(config.endpoint, "profile");
        assert_eq!(config.token.as_deref(), Some("flag"));
        assert_eq!(config.dialect, Dialect::Iceberg);
    }
}
