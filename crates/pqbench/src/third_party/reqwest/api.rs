//! Minimal async HTTP: one request in, one response out.

/// Errors from the HTTP transport.
#[derive(Debug)]
pub struct Error(String);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "reqwest: {}", self.0)
    }
}

impl std::error::Error for Error {}

impl From<String> for Error {
    fn from(message: String) -> Self {
        Self(message)
    }
}

/// One request: a URL, an optional bearer token, and an optional JSON body.
/// A body makes it a POST; without one it is a GET.
pub struct Request {
    /// The URL to request.
    pub url: String,
    /// The bearer credential, when the endpoint needs one.
    pub bearer: Option<String>,
    /// The JSON body; `None` sends a GET, `Some` sends a POST.
    pub body: Option<String>,
}

/// One response: the status and the body bytes.
pub struct Response {
    /// The HTTP status code.
    pub status: u16,
    /// The response body.
    pub bytes: Vec<u8>,
}

/// Send one request.
///
/// # Errors
/// Fails when the request cannot be sent.
pub async fn request(request: Request) -> Result<Response, Error> {
    super::r#impl::request(request).await
}
