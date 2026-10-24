//! Minimal async HTTP: one request in, one response out.

use std::fmt;

/// Errors from the HTTP transport.
#[derive(Debug)]
pub enum Error {
    /// The HTTP client could not be built.
    Client(Box<dyn std::error::Error + Send + Sync>),
    /// The request could not be sent.
    Transport(Box<dyn std::error::Error + Send + Sync>),
    /// The response body could not be read.
    Body(Box<dyn std::error::Error + Send + Sync>),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Client(_) => write!(f, "http client failed"),
            Error::Transport(_) => write!(f, "request failed"),
            Error::Body(_) => write!(f, "response failed"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Client(error) | Error::Transport(error) | Error::Body(error) => {
                Some(error.as_ref())
            }
        }
    }
}

/// The HTTP method one request uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    Get,
    Post,
    Head,
}

/// One request: a URL, a method, an optional bearer token, an optional JSON
/// body, and extra headers. Build one with [`Request::get`],
/// [`Request::post`], or [`Request::head`], then [`Request::header`].
pub struct Request {
    /// The URL to request.
    pub url: String,
    /// The HTTP method.
    pub method: Method,
    /// The bearer credential, when the endpoint needs one.
    pub bearer: Option<String>,
    /// The JSON body; `Some` sends a POST, `None` sends the method as is.
    pub body: Option<String>,
    /// Extra request headers, applied over the defaults.
    pub headers: Vec<(String, String)>,
}

impl Request {
    /// A GET request: the URL and an optional bearer.
    pub fn get(url: impl Into<String>, bearer: Option<String>) -> Self {
        Self {
            url: url.into(),
            method: Method::Get,
            bearer,
            body: None,
            headers: Vec::new(),
        }
    }

    /// A POST request: the URL, an optional bearer, and a JSON body.
    pub fn post(url: impl Into<String>, bearer: Option<String>, body: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            method: Method::Post,
            bearer,
            body: Some(body.into()),
            headers: Vec::new(),
        }
    }

    /// A HEAD request: the URL and an optional bearer. The response carries
    /// headers but no body.
    pub fn head(url: impl Into<String>, bearer: Option<String>) -> Self {
        Self {
            url: url.into(),
            method: Method::Head,
            bearer,
            body: None,
            headers: Vec::new(),
        }
    }

    /// Add one request header.
    #[must_use]
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }
}

/// One response: the status, the headers, and the body bytes.
pub struct Response {
    /// The HTTP status code.
    pub status: u16,
    /// The response headers.
    pub headers: Vec<(String, String)>,
    /// The response body.
    pub bytes: Vec<u8>,
}
