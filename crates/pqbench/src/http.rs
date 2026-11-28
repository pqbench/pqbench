//! The endpoint request path the resource commands share: one request, one
//! parsed JSON document; and [`paginate`], the page walk those documents are
//! listed through.
//!
//! The transport is [`crate::third_party::reqwest`]; this module owns the
//! response policy (a 200 status and a JSON body), so each command's `impl`
//! names neither the status nor the JSON error type. A workspace carrying an
//! HTTP GET hook (a reader's daemon) answers through the hook and the policy
//! still applies to that answer; [`fetch`] is the daemon's own side, which
//! reports the answer before any policy runs.

use std::fmt;
use std::future::Future;

use futures_util::stream::{self, Stream};
use futures_util::TryStreamExt;
use serde::de::DeserializeOwned;

use crate::third_party::reqwest::{Error as TransportError, Request, Response};
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

/// One GET returning a parsed JSON document, through the workspace's hook when
/// it carries one and the pooled transport otherwise.
pub(crate) async fn get_json<T: DeserializeOwned>(
    workspace: &Workspace,
    url: &str,
    token: Option<&str>,
) -> Result<T, Error> {
    let response = match workspace.http_get() {
        // The hook sends the request as whoever it speaks for, so the token the
        // caller's workspace would have used is not sent a second time.
        Some(http_get) => http_get(url.to_string()).await,
        None => {
            workspace
                .http()
                .request(Request::get(url, token.map(str::to_owned)))
                .await
        }
    }
    .map_err(Error::Transport)?;
    document(&response)
}

/// The parsed JSON document a 200 answer carries.
fn document<T: DeserializeOwned>(response: &Response) -> Result<T, Error> {
    if response.status != 200 {
        return Err(Error::Status {
            status: response.status,
            body: String::from_utf8_lossy(&response.bytes).into_owned(),
        });
    }
    serde_json::from_slice(&response.bytes).map_err(Error::Body)
}

/// One GET's answer, before any policy runs: the endpoint's status and a body
/// decoded as text.
pub struct Answer {
    /// The HTTP status.
    pub status: u16,
    /// The body, lossily decoded.
    pub body: String,
}

/// Send one GET with the workspace's own bearer and report the answer as the
/// endpoint gave it.
///
/// This is the daemon's side of the hook: it serves another process's catalog
/// GET under its own identity and returns the status unchanged, so the caller's
/// status policy and JSON decoding still apply to it.
///
/// # Errors
/// Fails when the request cannot be sent.
pub async fn fetch(workspace: &Workspace, url: &str) -> Result<Answer, Error> {
    let response = workspace
        .http()
        .request(Request::get(url, workspace.config().token.clone()))
        .await
        .map_err(Error::Transport)?;
    Ok(Answer {
        status: response.status,
        body: String::from_utf8_lossy(&response.bytes).into_owned(),
    })
}

/// The rows one page of a paginated listing reports, and the token that names
/// the page after it.
pub(crate) struct Page<T, Token> {
    /// The page's rows.
    pub(crate) rows: Vec<T>,
    /// The token of the next page; `None` on the endpoint's last page.
    pub(crate) next_page_token: Option<Token>,
}

/// Walk a paginated listing one page at a time: fetch a page, yield its rows,
/// then fetch the next with the token that page named.
///
/// `first_page_token` is the token of the first page (`None` when the listing
/// takes none), and `fetch_page` is the request for one page. The walk ends
/// when a page carries no next token; a page that fails is one `Err` item, so
/// the rows of the pages before it have already been yielded.
pub(crate) fn paginate<T, Token, E, F, Fut>(
    first_page_token: Option<Token>,
    fetch_page: F,
) -> impl Stream<Item = Result<T, E>>
where
    F: FnMut(Option<Token>) -> Fut,
    Fut: Future<Output = Result<Page<T, Token>, E>>,
{
    stream::try_unfold(
        (Some(first_page_token), fetch_page),
        |(page_token, mut fetch_page)| async move {
            let Some(page_token) = page_token else {
                return Ok(None);
            };
            let page = fetch_page(page_token).await?;
            // The next state is the token the page named; a last page names
            // none, which ends the walk.
            let rows = page.rows.into_iter().map(Ok).collect::<Vec<_>>();
            let next = page.next_page_token.map(Some);
            Ok(Some((rows, (next, fetch_page))))
        },
    )
    .map_ok(stream::iter)
    .try_flatten()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A workspace whose GETs answer with `status` and `body`.
    fn hooked(status: u16, body: &'static str) -> Workspace {
        Workspace::new(Default::default())
            .expect("a workspace")
            .with_http_get(move |url: String| async move {
                let document = serde_json::json!({ "url": url, "body": body });
                Ok(Response {
                    status,
                    headers: Vec::new(),
                    bytes: document.to_string().into_bytes(),
                })
            })
    }

    #[tokio::test]
    async fn a_hooked_workspace_reads_through_the_hook() {
        let workspace = hooked(200, "page");
        let value: serde_json::Value = get_json(&workspace, "http://endpoint.test/catalogs", None)
            .await
            .expect("the hook answers");
        assert_eq!(value["url"], "http://endpoint.test/catalogs");
    }

    #[tokio::test]
    async fn a_hooked_status_is_still_a_status_error() {
        let workspace = hooked(403, "denied");
        let error =
            get_json::<serde_json::Value>(&workspace, "http://endpoint.test/catalogs", None)
                .await
                .expect_err("a 403 is a failure");
        assert!(matches!(error, Error::Status { status: 403, .. }));
    }
}
