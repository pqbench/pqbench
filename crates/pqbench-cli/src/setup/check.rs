//! `pqbench setup --check`: the preflight of a run's configuration, its
//! credentials, and its catalog endpoint.
//!
//! Three checks, one line each: `config` (what the profile, the environment,
//! and the flags resolved — endpoint, dialect, and which credential the run
//! would send), `auth` (the bearer, minting a service principal's token when
//! that is the credential), and `catalog` (one page of the endpoint's first
//! listing level, under that bearer). A failed check never stops the next one,
//! so one run reports every problem; the command exits non-zero when any check
//! failed.
//!
//! Neither the client secret nor the bearer is printed: the configuration's
//! credential is described, never shown, and a mint reports only its lifetime.

use std::fmt;
use std::io::Write;

use pqbench::Workspace;

use crate::CliError;

/// The credential the resolved run sends.
#[derive(Clone, Copy)]
enum Credential {
    /// A bearer an operator supplied.
    Token,
    /// A bearer minted from a client id and secret.
    ServicePrincipal,
}

/// The preflight: one line per check, then the exit status.
pub(crate) async fn run(workspace: &Workspace) -> Result<(), CliError> {
    let mut report = Report::default();
    report.config(workspace.config())?;
    // The catalog check reads under the bearer the auth check settled on, so a
    // minted token is what proves the endpoint, exactly as the walk would.
    let workspace = authorize(workspace, &mut report).await?;
    if !workspace.config().endpoint.is_empty() {
        reach(&workspace, &mut report).await?;
    }
    report.finish()
}

/// The checks, as the lines they printed.
#[derive(Default)]
struct Report {
    /// How many checks failed, which is the exit status.
    failures: usize,
}

impl Report {
    /// The configuration check: what the layers resolved, and the credential
    /// the run would send.
    fn config(&mut self, config: &pqbench::Config) -> Result<(), CliError> {
        if config.endpoint.is_empty() {
            return self.failed(
                "config",
                "no endpoint: --endpoint, PQB_ENDPOINT, DATABRICKS_HOST, or a ~/.pqbenchrc profile",
            );
        }
        let credential = match credential(config) {
            Some(Credential::Token) => "a bearer token",
            Some(Credential::ServicePrincipal) => "a service principal's client id and secret",
            None => "no credential",
        };
        self.passed(
            "config",
            format!(
                "endpoint {}, dialect {}, {credential}",
                config.endpoint,
                config.dialect.as_str()
            ),
        )
    }

    fn passed(&mut self, check: &str, detail: impl fmt::Display) -> Result<(), CliError> {
        self.line("ok", check, detail)
    }

    fn failed(&mut self, check: &str, detail: impl fmt::Display) -> Result<(), CliError> {
        self.line("fail", check, detail)?;
        self.failures += 1;
        Ok(())
    }

    /// One report line, written through stdout with a closed pipe treated as
    /// nothing to say: `| head` is not a failed check.
    fn line(
        &mut self,
        status: &str,
        check: &str,
        detail: impl fmt::Display,
    ) -> Result<(), CliError> {
        let mut out = std::io::stdout().lock();
        match writeln!(out, "{status:<5} {check:<8} {detail}") {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::BrokenPipe => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    /// The command's result: green when every check passed, and otherwise the
    /// count the lines already explained.
    fn finish(self) -> Result<(), CliError> {
        match self.failures {
            0 => Ok(()),
            failures => Err(format!("{failures} setup check(s) failed").into()),
        }
    }
}

/// The auth check: the bearer the run would send, minted when the credential is
/// a service principal. The workspace it returns carries a minted bearer, so
/// the catalog check reads under it.
async fn authorize(workspace: &Workspace, report: &mut Report) -> Result<Workspace, CliError> {
    match credential(workspace.config()) {
        Some(Credential::Token) => {
            report.passed("auth", "the configuration's bearer token")?;
            Ok(workspace.clone())
        }
        Some(Credential::ServicePrincipal) => match pqbench::oauth::mint(workspace).await {
            Ok(token) => {
                report.passed(
                    "auth",
                    format!(
                        "service-principal token minted, expires in {} s",
                        token.lifetime().as_secs()
                    ),
                )?;
                Ok(workspace.with_token(token.access_token()))
            }
            Err(error) => {
                report.failed("auth", summary(&error))?;
                Ok(workspace.clone())
            }
        },
        None => {
            report.failed(
                "auth",
                "no credential: --token, PQB_TOKEN, DATABRICKS_TOKEN, or \
                 --client-id/--client-secret (PQB_CLIENT_ID, PQB_CLIENT_SECRET)",
            )?;
            Ok(workspace.clone())
        }
    }
}

/// The reachability check: one page of the endpoint's first listing level, the
/// same route the walk's first stage reads.
async fn reach(workspace: &Workspace, report: &mut Report) -> Result<(), CliError> {
    let config = workspace.config();
    let url = config.dialect.listing_url(&config.endpoint);
    match pqbench::http::fetch(workspace, &url).await {
        Ok(answer) if answer.status == 200 => {
            report.passed("catalog", format!("GET {url} answered 200"))
        }
        Ok(answer) => report.failed("catalog", format!("GET {url} answered {}", answer.status)),
        Err(error) => report.failed("catalog", format!("GET {url} failed: {}", summary(&error))),
    }
}

/// The credential the resolved run sends; a static token wins over a service
/// principal, so a workspace configured with both sends the token an operator
/// handed it.
fn credential(config: &pqbench::Config) -> Option<Credential> {
    if config.token.is_some() {
        return Some(Credential::Token);
    }
    let service_principal = config.client_id.is_some() && config.client_secret.is_some();
    service_principal.then_some(Credential::ServicePrincipal)
}

/// One line for a failure: the error and its causes, so a check's line stays a
/// line.
fn summary(error: &dyn std::error::Error) -> String {
    crate::error::error_chain(error).replace("\n  caused by: ", ": ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(token: Option<&str>, client_id: Option<&str>) -> pqbench::Config {
        pqbench::Config {
            token: token.map(str::to_string),
            client_id: client_id.map(str::to_string),
            client_secret: client_id.map(pqbench::Secret::new),
            ..pqbench::Config::default()
        }
    }

    #[test]
    fn a_static_token_wins_over_a_service_principal() {
        assert!(matches!(
            credential(&config(Some("dapi"), Some("id"))),
            Some(Credential::Token)
        ));
    }

    #[test]
    fn a_client_id_and_secret_alone_is_a_service_principal() {
        assert!(matches!(
            credential(&config(None, Some("id"))),
            Some(Credential::ServicePrincipal)
        ));
        assert!(credential(&config(None, None)).is_none());
    }
}
