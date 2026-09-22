//! Shared HTTP for catalog list clients.
//!
//! `GET /v1/config` chooses the protocol: a 200 with a `defaults` object is
//! Iceberg REST; a 200 without `defaults`, or HTTP 404, is Unity. Transport
//! failures and other HTTP statuses propagate so a down catalog is not listed
//! as Unity.

use serde::Deserialize;

use crate::CliError;

pub(crate) const PAGE_CAP: usize = 32;

/// Catalog protocol selected from `GET /v1/config`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Protocol {
    Unity,
    IcebergRest,
}

#[derive(Deserialize)]
struct Config {
    #[serde(default)]
    defaults: Option<serde_json::Value>,
}

/// Choose Unity or Iceberg REST from `GET {endpoint}/v1/config`.
pub(crate) fn protocol(endpoint: &str, token: Option<&str>) -> Result<Protocol, CliError> {
    let url = format!("{}/v1/config", endpoint.trim_end_matches('/'));
    match get_body(&url, token) {
        Ok(body) => {
            let config: Config = serde_json::from_str(&body).map_err(|error| {
                format!("catalog response was not the expected document: {error}")
            })?;
            if config.defaults.is_some() {
                Ok(Protocol::IcebergRest)
            } else {
                Ok(Protocol::Unity)
            }
        }
        Err(GetError::Status { code: 404, .. }) => Ok(Protocol::Unity),
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn get_json<T: for<'de> Deserialize<'de>>(
    url: &str,
    token: Option<&str>,
) -> Result<T, CliError> {
    let body = get_body(url, token)?;
    serde_json::from_str(&body)
        .map_err(|error| format!("catalog response was not the expected document: {error}").into())
}

enum GetError {
    Status { code: u16, body: String },
    Transport(String),
    Text(String),
}

impl From<GetError> for CliError {
    fn from(error: GetError) -> Self {
        match error {
            GetError::Status { code, body } => {
                format!("catalog returned HTTP {code}: {body}").into()
            }
            GetError::Transport(message) => format!("catalog request failed: {message}").into(),
            GetError::Text(message) => format!("catalog response was not text: {message}").into(),
        }
    }
}

fn get_body(url: &str, token: Option<&str>) -> Result<String, GetError> {
    let request = ureq::get(url);
    let request = match token {
        Some(token) => request.set("Authorization", &format!("Bearer {token}")),
        None => request,
    };
    let response = request.call().map_err(|error| match error {
        ureq::Error::Status(code, response) => {
            let body = match response.into_string() {
                Ok(body) => body,
                Err(error) => {
                    return GetError::Text(format!(
                        "catalog returned HTTP {code} and the body could not be read: {error}"
                    ));
                }
            };
            GetError::Status { code, body }
        }
        other => GetError::Transport(other.to_string()),
    })?;
    response
        .into_string()
        .map_err(|error| GetError::Text(error.to_string()))
}

pub(crate) fn encode(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char);
            }
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}
