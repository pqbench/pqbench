//! `pqbench setup`: print the walk's environment for the caller to `eval`.
//!
//! A child process cannot set its parent's environment, so `setup` prints the
//! `export` lines instead:
//!
//! ```console no-run
//! $ eval "$(pqbench setup)"
//! ```
//!
//! The endpoint and token resolve the way the Databricks SDKs do: the flag,
//! then `PQB_ENDPOINT` / `PQB_TOKEN`, then `DATABRICKS_HOST` /
//! `DATABRICKS_TOKEN`. The notebook kernel's `dbutils` context is not visible
//! to a subprocess, so a notebook exports `DATABRICKS_*` from it first. The
//! region comes from the flag or `AWS_REGION`; the vended lease carries none.

use clap::Args;

use crate::CliError;

/// Arguments for `setup`.
#[derive(Args)]
pub(crate) struct SetupArgs {
    /// workspace URL; defaults to PQB_ENDPOINT, then DATABRICKS_HOST
    #[arg(long, value_name = "URL")]
    endpoint: Option<String>,
    /// bearer token; defaults to PQB_TOKEN, then DATABRICKS_TOKEN
    #[arg(long, value_name = "TOKEN")]
    token: Option<String>,
    /// catalog dialect: unity (the default) or iceberg
    #[arg(long, value_name = "FORMAT")]
    table_format: Option<String>,
    /// bucket region; defaults to AWS_REGION
    #[arg(long, value_name = "REGION")]
    region: Option<String>,
}

pub(crate) fn run(args: &SetupArgs) -> Result<(), CliError> {
    let endpoint = resolve(&args.endpoint, &["PQB_ENDPOINT", "DATABRICKS_HOST"])
        .ok_or("setup needs a workspace URL: --endpoint, PQB_ENDPOINT, or DATABRICKS_HOST")?;
    let endpoint = endpoint.trim_end_matches('/').to_string();
    let token = resolve(&args.token, &["PQB_TOKEN", "DATABRICKS_TOKEN"])
        .ok_or("setup needs a token: --token, PQB_TOKEN, or DATABRICKS_TOKEN")?;
    let table_format = resolve(&args.table_format, &["PQB_TABLE_FORMAT"]);
    if let Some(format) = &table_format {
        if !matches!(format.as_str(), "unity" | "iceberg") {
            return Err(
                format!("unknown table format {format:?}; expected unity or iceberg").into(),
            );
        }
    }
    let region = resolve(&args.region, &["AWS_REGION"]);

    let mut out = String::new();
    export(&mut out, "PQB_ENDPOINT", &endpoint);
    export(&mut out, "PQB_TOKEN", &token);
    if let Some(format) = &table_format {
        export(&mut out, "PQB_TABLE_FORMAT", format);
    }
    if let Some(region) = &region {
        export(&mut out, "AWS_REGION", region);
    }
    export(&mut out, "AWS_EC2_METADATA_DISABLED", "true");
    print!("{out}");
    Ok(())
}

/// The first non-empty value: the flag, then each environment variable.
fn resolve(flag: &Option<String>, names: &[&str]) -> Option<String> {
    flag.clone()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            names
                .iter()
                .find_map(|name| std::env::var(name).ok())
                .filter(|value| !value.trim().is_empty())
        })
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

    #[test]
    fn the_flag_wins() {
        std::env::set_var("PQB_TEST_SETUP_ENDPOINT", "from-env");
        assert_eq!(
            resolve(&Some("from-flag".into()), &["PQB_TEST_SETUP_ENDPOINT"]).as_deref(),
            Some("from-flag")
        );
        std::env::remove_var("PQB_TEST_SETUP_ENDPOINT");
    }
}
