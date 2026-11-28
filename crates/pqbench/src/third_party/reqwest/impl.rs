//! The `reqwest` backend behind [`super::api`].
//!
//! `reqwest` is the only crate named here. [`Http`] wraps the one client the
//! shared [`crate::Workspace`] holds, so connections stay pooled across a
//! command's requests.

use std::time::Duration;

use reqwest::Client;

use super::api::{Error, Method, Request, Response};

/// The one pooled client, built once at startup by [`crate::Workspace::new`].
/// Cloning is cheap — the client is an `Arc` inside.
#[derive(Debug, Clone)]
pub(crate) struct Http(Client);

impl Http {
    /// Build the client, bounding each request when the caller set a timeout.
    pub(crate) fn new(request_timeout: Option<Duration>) -> Result<Self, Error> {
        let mut builder = Client::builder();
        if let Some(timeout) = request_timeout {
            builder = builder.connect_timeout(timeout).read_timeout(timeout);
        }
        let client = builder
            .build()
            .map_err(|error| Error::Client(Box::new(error)))?;
        Ok(Self(client))
    }

    /// The underlying client, for the object-store connector that must share
    /// this one pool.
    #[cfg(feature = "aws")]
    pub(crate) fn client(&self) -> &Client {
        &self.0
    }

    pub(crate) async fn request(&self, request: Request) -> Result<Response, Error> {
        let client = &self.0;
        let mut builder = match request.method {
            Method::Post => client
                .post(&request.url)
                .body(request.body.clone().unwrap_or_default()),
            Method::Get => client.get(&request.url),
            Method::Head => client.head(&request.url),
        };
        // A POST carries JSON unless the caller's own header names another
        // type, like the form body an OAuth token request sends.
        let content_type_given = request
            .headers
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case("content-type"));
        if request.method == Method::Post && !content_type_given {
            builder = builder.header(reqwest::header::CONTENT_TYPE, "application/json");
        }
        if let Some(bearer) = &request.bearer {
            builder = builder.bearer_auth(bearer);
        }
        for (name, value) in &request.headers {
            builder = builder.header(name.as_str(), value.as_str());
        }
        let response = builder
            .send()
            .await
            .map_err(|error| Error::Transport(Box::new(error)))?;
        let status = response.status().as_u16();
        let headers: Vec<(String, String)> = response
            .headers()
            .iter()
            .filter_map(|(name, value)| {
                Some((name.as_str().to_string(), value.to_str().ok()?.to_string()))
            })
            .collect();
        let body = response
            .bytes()
            .await
            .map(|bytes| bytes.to_vec())
            .map_err(|error| Error::Body(Box::new(error)))?;
        Ok(Response {
            status,
            headers,
            bytes: body,
        })
    }
}
