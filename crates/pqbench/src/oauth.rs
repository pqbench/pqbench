//! Databricks service-principal OAuth M2M: one minted bearer for the catalog.
//!
//! A workspace configured with a client id and a client secret, and no static
//! token, authenticates as a service principal. The bearer is minted from
//! `POST <endpoint>/oidc/v1/token` — `grant_type=client_credentials`,
//! `scope=all-apis`, HTTP Basic over base64 `client_id:client_secret` — and
//! lives for the `expires_in` seconds the endpoint reports. [`mint`] is that
//! one call.
//!
//! A caller that serves many requests holds the [`Token`] and asks
//! [`Token::fresh`] whether it may still send it: a token is replaced five
//! minutes before it expires, or at half its lifetime when that is shorter than
//! ten minutes, the same margin the lease cache uses. So a one-hour token is
//! minted once an hour plus a token request, and a walk never sends an expired
//! one.
//!
//! Nothing here prints a credential: [`crate::config::Secret`] and [`Token`]
//! render redacted, and a failed mint reports only the status and the OAuth
//! error document's `error`/`error_description`.

use std::fmt;
use std::time::{Duration, Instant};

use base64::Engine as _;
use serde::Deserialize;

use crate::config::Secret;
use crate::dialect;
use crate::third_party::reqwest::{Request, Response};
use crate::Workspace;

/// The token request's body, as `application/x-www-form-urlencoded`: a
/// client-credentials grant for every workspace API.
const REQUEST_BODY: &str = "grant_type=client_credentials&scope=all-apis";

/// How long a token lives when the endpoint reports no `expires_in`; an hour is
/// the lifetime a Databricks workspace mints.
const ASSUMED_LIFETIME: Duration = Duration::from_secs(60 * 60);

/// How long before expiry a token is replaced.
const REFRESH_MARGIN: Duration = Duration::from_secs(5 * 60);

/// Errors minting a bearer.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The configuration names no client id and secret to mint with.
    Credentials,
    /// The request could not be sent.
    Transport(crate::third_party::reqwest::Error),
    /// The endpoint answered a status other than 200.
    Status {
        /// The HTTP status.
        status: u16,
        /// The OAuth error the endpoint reported, when its body named one.
        detail: String,
    },
    /// The answer was not a token document.
    Body(serde_json::Error),
    /// The answer carried an empty access token.
    EmptyToken,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Credentials => f.write_str(
                "no service-principal credentials: --client-id and --client-secret, \
                 PQB_CLIENT_ID and PQB_CLIENT_SECRET, or client-id/client-secret in the profile",
            ),
            Error::Transport(_) => f.write_str("the token request could not be sent"),
            Error::Status { status, detail } => {
                write!(f, "the token endpoint answered HTTP {status}{detail}")
            }
            Error::Body(_) => f.write_str("the token endpoint's answer was not a token document"),
            Error::EmptyToken => f.write_str("the token endpoint answered an empty access token"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Transport(error) => Some(error),
            Error::Body(error) => Some(error),
            Error::Credentials | Error::Status { .. } | Error::EmptyToken => None,
        }
    }
}

/// One minted bearer and the lifetime its endpoint reported.
///
/// The token itself is only ever sent, never printed: `Debug` names the
/// lifetime and nothing else.
pub struct Token {
    access_token: String,
    lifetime: Duration,
}

impl Token {
    /// The bearer to send on a catalog request.
    #[must_use]
    pub fn access_token(&self) -> &str {
        &self.access_token
    }

    /// How long the endpoint said the token lives.
    #[must_use]
    pub fn lifetime(&self) -> Duration {
        self.lifetime
    }

    /// Whether the token minted at `minted` may still be sent at `now`: it is
    /// replaced five minutes before it expires, or at half its lifetime when
    /// that is shorter than ten minutes.
    #[must_use]
    pub fn fresh(&self, minted: Instant, now: Instant) -> bool {
        let margin = REFRESH_MARGIN.min(self.lifetime / 2);
        now.saturating_duration_since(minted) < self.lifetime.saturating_sub(margin)
    }
}

impl fmt::Debug for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Token")
            .field("access_token", &"<redacted>")
            .field("lifetime", &self.lifetime)
            .finish()
    }
}

/// The token document the endpoint answers with.
#[derive(Deserialize)]
struct Minted {
    access_token: String,
    /// Seconds the token lives; a document without one is taken at the
    /// workspace's usual hour.
    #[serde(default, rename = "expires_in")]
    expires_seconds: Option<u64>,
}

/// The OAuth error document a non-200 answer carries. Only these two fields
/// are read, so nothing else an endpoint echoes reaches an error message.
#[derive(Deserialize, Default)]
struct ErrorDocument {
    #[serde(default)]
    error: String,
    #[serde(default)]
    error_description: String,
}

/// Mint the workspace's bearer from its client id and secret.
///
/// # Errors
/// Fails when the configuration names no client id and secret, when the request
/// cannot be sent, and when the endpoint refuses it or answers no token.
pub async fn mint(workspace: &Workspace) -> Result<Token, Error> {
    let config = workspace.config();
    let (Some(client_id), Some(client_secret)) =
        (config.client_id.as_deref(), config.client_secret.as_ref())
    else {
        return Err(Error::Credentials);
    };
    let url = format!(
        "{}/oidc/v1/token",
        dialect::workspace_root(&config.endpoint)
    );
    let request = Request::post(url, None, REQUEST_BODY)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .header("Authorization", basic(client_id, client_secret));
    let response = workspace
        .http()
        .request(request)
        .await
        .map_err(Error::Transport)?;
    token(&response)
}

/// The token a response carries, or the failure it reports.
fn token(response: &Response) -> Result<Token, Error> {
    if response.status != 200 {
        return Err(Error::Status {
            status: response.status,
            detail: detail(&response.bytes),
        });
    }
    let minted: Minted = serde_json::from_slice(&response.bytes).map_err(Error::Body)?;
    if minted.access_token.is_empty() {
        return Err(Error::EmptyToken);
    }
    Ok(Token {
        access_token: minted.access_token,
        lifetime: minted
            .expires_seconds
            .map_or(ASSUMED_LIFETIME, Duration::from_secs),
    })
}

/// The OAuth error a refusal named: `: error: description`, or nothing when the
/// body was not an OAuth error document.
fn detail(body: &[u8]) -> String {
    let Ok(document) = serde_json::from_slice::<ErrorDocument>(body) else {
        return String::new();
    };
    match (
        document.error.is_empty(),
        document.error_description.is_empty(),
    ) {
        (true, _) => String::new(),
        (false, true) => format!(": {}", document.error),
        (false, false) => format!(": {}: {}", document.error, document.error_description),
    }
}

/// The `Authorization` header of a client-credentials request: HTTP Basic over
/// `client_id:client_secret`.
fn basic(client_id: &str, secret: &Secret) -> String {
    let credentials = format!("{client_id}:{}", secret.expose());
    format!("Basic {}", encode_base64(credentials.as_bytes()))
}

/// `bytes` as padded Base64: HTTP Basic carries its credentials in the standard
/// alphabet (RFC 4648 §4).
fn encode_base64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::{Read, Write};
    use std::sync::{Arc, Mutex};

    use crate::Config;

    /// One request the fake token endpoint read: its request line, its headers,
    /// and its body.
    #[derive(Clone, Default)]
    struct Received {
        request_line: String,
        headers: Vec<String>,
        body: String,
    }

    impl Received {
        /// The value of one request header, as the endpoint read it.
        fn header(&self, name: &str) -> Option<&str> {
            self.headers.iter().find_map(|header| {
                let (key, value) = header.split_once(':')?;
                (key.eq_ignore_ascii_case(name)).then_some(value.trim())
            })
        }
    }

    /// A fake token endpoint answering `status` and `body` to every request,
    /// and recording what it read.
    fn endpoint(status: u16, body: &'static str) -> (String, Arc<Mutex<Vec<Received>>>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a listener");
        let address = format!("http://{}", listener.local_addr().expect("an address"));
        let received = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&received);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                let mut buffer = [0u8; 8192];
                let read = stream.read(&mut buffer).unwrap_or(0);
                let request = String::from_utf8_lossy(&buffer[..read]).to_string();
                let (head, request_body) = request.split_once("\r\n\r\n").unwrap_or((&request, ""));
                let mut lines = head.lines();
                let request_line = lines.next().unwrap_or_default().to_string();
                let headers = lines.map(str::to_string).collect();
                recorded.lock().expect("the record lock").push(Received {
                    request_line,
                    headers,
                    body: request_body.to_string(),
                });
                let response = format!(
                    "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes());
            }
        });
        (address, received)
    }

    fn workspace(endpoint: &str, client_secret: &str) -> Workspace {
        Workspace::new(Config {
            endpoint: endpoint.to_string(),
            token: None,
            dialect: crate::Dialect::Unity,
            request_timeout: None,
            client_id: Some("client-id".to_string()),
            client_secret: Some(Secret::new(client_secret)),
        })
        .expect("the workspace builds")
    }

    #[tokio::test]
    async fn a_service_principal_mints_a_bearer_with_client_credentials() {
        let (address, received) = endpoint(
            200,
            r#"{"access_token":"minted-bearer","token_type":"Bearer","expires_in":3600}"#,
        );
        let token = mint(&workspace(&address, "s3cret"))
            .await
            .expect("the endpoint mints");
        assert_eq!(token.access_token(), "minted-bearer");
        assert_eq!(token.lifetime(), Duration::from_secs(3600));

        let received = received.lock().expect("the record lock").clone();
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].request_line, "POST /oidc/v1/token HTTP/1.1");
        assert_eq!(
            received[0].header("content-type"),
            Some("application/x-www-form-urlencoded")
        );
        assert_eq!(
            received[0].header("authorization"),
            Some(format!("Basic {}", encode_base64(b"client-id:s3cret")).as_str())
        );
        assert!(received[0].body.contains("grant_type=client_credentials"));
        assert!(received[0].body.contains("scope=all-apis"));
    }

    /// The token route hangs off the workspace root, not off the catalog base:
    /// an Iceberg REST endpoint still mints from `{host}/oidc/v1/token`.
    #[tokio::test]
    async fn the_token_route_hangs_off_the_workspace_root() {
        let (address, received) = endpoint(200, r#"{"access_token":"b","expires_in":60}"#);
        let endpoint = format!("{address}/iceberg-rest/v1");
        mint(&workspace(&endpoint, "s")).await.expect("minted");
        let received = received.lock().expect("the record lock").clone();
        assert_eq!(received[0].request_line, "POST /oidc/v1/token HTTP/1.1");
    }

    #[tokio::test]
    async fn a_document_without_expires_in_is_taken_at_the_usual_hour() {
        let (address, _) = endpoint(200, r#"{"access_token":"b"}"#);
        let token = mint(&workspace(&address, "s")).await.expect("minted");
        assert_eq!(token.lifetime(), ASSUMED_LIFETIME);
    }

    #[tokio::test]
    async fn a_refusal_reports_the_oauth_error_and_nothing_else() {
        // The document carries the credentials' echo; only `error` and
        // `error_description` may reach the message.
        let (address, _) = endpoint(
            401,
            r#"{"error":"invalid_client","error_description":"no such client","echo":"s3cret"}"#,
        );
        let error = mint(&workspace(&address, "s3cret"))
            .await
            .expect_err("a 401 mints nothing");
        let message = error.to_string();
        assert!(message.contains("HTTP 401"), "{message}");
        assert!(message.contains("invalid_client"), "{message}");
        assert!(message.contains("no such client"), "{message}");
        assert!(
            !message.contains("s3cret"),
            "the secret was reported: {message}"
        );
    }

    #[tokio::test]
    async fn an_empty_access_token_is_rejected() {
        let (address, _) = endpoint(200, r#"{"access_token":"","expires_in":3600}"#);
        let error = mint(&workspace(&address, "s"))
            .await
            .expect_err("an empty token mints nothing");
        assert!(matches!(error, Error::EmptyToken), "{error}");
    }

    #[tokio::test]
    async fn a_body_that_is_not_a_token_document_is_an_error() {
        let (address, _) = endpoint(200, r#"{"token_type":"Bearer"}"#);
        let error = mint(&workspace(&address, "s"))
            .await
            .expect_err("no access token");
        assert!(matches!(error, Error::Body(_)), "{error}");
    }

    #[tokio::test]
    async fn a_workspace_without_client_credentials_mints_nothing() {
        let workspace = Workspace::new(Config::default()).expect("the workspace builds");
        let error = mint(&workspace).await.expect_err("no credentials");
        assert!(matches!(error, Error::Credentials), "{error}");
    }

    #[test]
    fn neither_the_secret_nor_the_bearer_is_printed() {
        let token = Token {
            access_token: "minted-bearer".to_string(),
            lifetime: Duration::from_secs(60),
        };
        assert!(!format!("{token:?}").contains("minted-bearer"));
        let config = Config {
            client_id: Some("client-id".to_string()),
            client_secret: Some(Secret::new("s3cret")),
            ..Config::default()
        };
        assert!(!format!("{config:?}").contains("s3cret"));
        assert!(format!("{config:?}").contains("client-id"));
    }

    #[test]
    fn a_token_is_replaced_before_it_expires() {
        let now = Instant::now();
        let hour = Token {
            access_token: "a".to_string(),
            lifetime: Duration::from_secs(60 * 60),
        };
        assert!(hour.fresh(now, now + Duration::from_secs(54 * 60)));
        assert!(!hour.fresh(now, now + Duration::from_secs(56 * 60)));

        let short = Token {
            access_token: "a".to_string(),
            lifetime: Duration::from_secs(8 * 60),
        };
        assert!(short.fresh(now, now + Duration::from_secs(3 * 60)));
        assert!(!short.fresh(now, now + Duration::from_secs(5 * 60)));

        // A token minted with no lifetime is already spent, so the next
        // request mints another.
        let spent = Token {
            access_token: "a".to_string(),
            lifetime: Duration::ZERO,
        };
        assert!(!spent.fresh(now, now));
    }
}
