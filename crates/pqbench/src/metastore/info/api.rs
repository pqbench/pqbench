//! `metastore info`: the endpoint's metastore record.
//!
//! [`read`] is the only public function. It returns plain data; the CLI owns
//! the document it becomes.

use std::fmt;

/// One metastore record.
#[derive(Debug, Clone)]
pub struct Metastore {
    /// The metastore name.
    pub name: String,
    /// The metastore id.
    pub id: String,
    /// The cloud the metastore lives in.
    pub cloud: String,
    /// The cloud region the metastore lives in.
    pub region: String,
}

/// Errors reading a metastore.
#[derive(Debug)]
pub struct Error(String);

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "metastore: {}", self.0)
    }
}

impl std::error::Error for Error {}

impl From<String> for Error {
    fn from(message: String) -> Self {
        Self(message)
    }
}

/// Read the metastore record from `endpoint`.
///
/// # Errors
/// Fails when the endpoint cannot be reached, answers with an unexpected
/// status, or returns a malformed summary.
pub async fn read(endpoint: &str, token: Option<&str>) -> Result<Metastore, Error> {
    super::r#impl::read(endpoint, token).await
}
