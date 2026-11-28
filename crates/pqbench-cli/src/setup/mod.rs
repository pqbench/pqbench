//! `pqbench setup`: print the resolved configuration for the shell to `eval`,
//! or check it with `--check`.
//!
//! A child process cannot set its parent's environment, so `setup` prints the
//! `export` lines instead:
//!
//! ```text
//! $ eval "$(pqbench setup)"
//! ```
//!
//! The endpoint and token come from the resolved configuration — the flag, then
//! `PQB_*`, then `DATABRICKS_*`, then `~/.pqbenchrc`. A workspace configured
//! with a service principal's client id and secret and no static token mints
//! its bearer for the export, so the shell carries a token that works now. The
//! secret is never printed: the daemon that needs to refresh the token reads
//! the client credentials from the configuration itself. A notebook kernel's
//! `dbutils` workspace is not visible to a subprocess, so a notebook exports
//! `DATABRICKS_*` from it first. `AWS_REGION` comes from the flag or the
//! environment; the vended lease carries none.

mod check;

use clap::Args;
use pqbench::Workspace;

use crate::CliError;

/// Arguments for `setup`: the bucket region, which the walk passes through to
/// the storage client, and the preflight switch.
#[derive(Args)]
pub(crate) struct SetupArgs {
    /// bucket region [env: AWS_REGION]
    #[arg(long, value_name = "REGION", env = "AWS_REGION")]
    region: Option<String>,
    /// check the configuration, the credentials, and the endpoint instead of
    /// printing exports
    #[arg(long)]
    check: bool,
}

pub(crate) async fn run(workspace: &Workspace, args: &SetupArgs) -> Result<(), CliError> {
    if args.check {
        return check::run(workspace).await;
    }
    let config = workspace.config();
    if config.endpoint.is_empty() {
        return Err(
            "setup needs an endpoint: --endpoint, PQB_ENDPOINT, DATABRICKS_HOST, or ~/.pqbenchrc"
                .into(),
        );
    }
    let token = match config.token.clone() {
        Some(token) => token,
        None => pqbench::oauth::mint(workspace)
            .await?
            .access_token()
            .to_string(),
    };

    let mut out = String::new();
    export(&mut out, "PQB_ENDPOINT", &config.endpoint);
    export(&mut out, "PQB_TOKEN", &token);
    export(&mut out, "PQB_TABLE_FORMAT", config.dialect.as_str());
    if let Some(region) = non_empty(args.region.as_deref()) {
        export(&mut out, "AWS_REGION", region);
    }
    export(&mut out, "AWS_EC2_METADATA_DISABLED", "true");
    crate::render::write(&out).await
}

/// A value with the surrounding whitespace trimmed; blank is nothing.
fn non_empty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

/// One `export NAME='value'` line.
fn export(out: &mut String, name: &str, value: &str) {
    out.push_str("export ");
    out.push_str(name);
    out.push('=');
    out.push_str(&quote(value));
    out.push('\n');
}

/// Single-quote a shell word, escaping embedded quotes.
fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_quote_is_escaped() {
        assert_eq!(quote("it's"), r#"'it'\''s'"#);
    }
}
