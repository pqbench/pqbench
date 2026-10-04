//! The `reqwest` backend behind [`super::api`].
//!
//! `reqwest` is the only crate named here. One process-wide client is reused,
//! so connections stay pooled across a command's requests.

use std::sync::OnceLock;
use std::time::Duration;

use reqwest::Client;

use super::api::{Error, Method, Request, Response};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// One pooled client for every request.
fn client() -> Result<&'static Client, Error> {
    static CLIENT: OnceLock<Result<Client, String>> = OnceLock::new();
    CLIENT
        .get_or_init(|| {
            Client::builder()
                .timeout(REQUEST_TIMEOUT)
                .build()
                .map_err(|error| error.to_string())
        })
        .as_ref()
        .map_err(|error| Error::from(format!("http client: {error}")))
}

pub(crate) async fn request(request: Request) -> Result<Response, Error> {
    let client = client()?;
    let mut builder = match request.method {
        Method::Post => client
            .post(&request.url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(request.body.clone().unwrap_or_default()),
        Method::Get => client.get(&request.url),
        Method::Head => client.head(&request.url),
    };
    if let Some(bearer) = &request.bearer {
        builder = builder.bearer_auth(bearer);
    }
    for (name, value) in &request.headers {
        builder = builder.header(name.as_str(), value.as_str());
    }
    let response = builder
        .send()
        .await
        .map_err(|error| Error::from(format!("request failed: {error}")))?;
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
        .map_err(|error| Error::from(format!("response failed: {error}")))?;
    Ok(Response {
        status,
        headers,
        bytes: body,
    })
}
