//! The endpoint request path the resource commands share: one request, one
//! parsed JSON document.
//!
//! The transport is [`crate::third_party::reqwest`]; this module owns the
//! response policy (a 200 status and a JSON body), so each command's `impl`
//! names neither the status nor the JSON error type.

use std::fmt;

use serde::de::DeserializeOwned;

use crate::third_party::reqwest::{Error as TransportError, Request};
use crate::Workspace;

/// Errors from an endpoint request.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The request could not be sent.
    Transport(TransportError),
    /// The endpoint answered with a non-200 status.
    Status {
        /// The HTTP status.
        status: u16,
        /// The response body, lossily decoded.
        body: String,
    },
    /// The response body was not the expected JSON document.
    Body(serde_json::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Transport(_) => write!(f, "the request could not be sent"),
            Error::Status { status, body } => {
                write!(f, "the endpoint returned HTTP {status}: {body}")
            }
            Error::Body(_) => write!(f, "the response was not the expected document"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Transport(error) => Some(error),
            Error::Body(error) => Some(error),
            Error::Status { .. } => None,
        }
    }
}

/// One GET returning a parsed JSON document.
pub(crate) async fn get_json<T: DeserializeOwned>(
    workspace: &Workspace,
    url: &str,
    token: Option<&str>,
) -> Result<T, Error> {
    send_json(workspace, Request::get(url, token.map(str::to_owned))).await
}

/// Send one request and parse its JSON body.
pub(crate) async fn send_json<T: DeserializeOwned>(
    workspace: &Workspace,
    request: Request,
) -> Result<T, Error> {
    let response = workspace.request(request).await.map_err(Error::Transport)?;
    if response.status != 200 {
        return Err(Error::Status {
            status: response.status,
            body: String::from_utf8_lossy(&response.bytes).into_owned(),
        });
    }
    serde_json::from_slice(&response.bytes).map_err(Error::Body)
}
