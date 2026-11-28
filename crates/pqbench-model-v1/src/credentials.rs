use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Vended storage credentials. They are session state, not resource data: the
/// reader holds them in memory and they never ride a [`Table`](super::Table)
/// or [`TableFile`](super::TableFile).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Credentials {
    /// The access key id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_key_id: Option<String>,
    /// The secret access key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_access_key: Option<String>,
    /// The session token, when the lease is temporary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_token: Option<String>,
    /// When the lease expires, milliseconds since the Unix epoch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expire_time: Option<u64>,
}

impl Credentials {
    /// The lease those `AWS_*` options name.
    #[must_use]
    pub fn from_env(env: &BTreeMap<String, String>) -> Self {
        Self {
            access_key_id: env.get("AWS_ACCESS_KEY_ID").cloned(),
            secret_access_key: env.get("AWS_SECRET_ACCESS_KEY").cloned(),
            session_token: env.get("AWS_SESSION_TOKEN").cloned(),
            expire_time: None,
        }
    }

    /// The `AWS_*` options to read storage with. Non-credential options
    /// (`AWS_REGION`, `AWS_ENDPOINT`, flags) are configuration and come from
    /// the process environment, not the wire.
    #[must_use]
    pub fn to_env(&self) -> BTreeMap<String, String> {
        [
            ("AWS_ACCESS_KEY_ID", &self.access_key_id),
            ("AWS_SECRET_ACCESS_KEY", &self.secret_access_key),
            ("AWS_SESSION_TOKEN", &self.session_token),
        ]
        .into_iter()
        .filter_map(|(key, value)| Some((key.to_string(), value.clone()?)))
        .collect()
    }

    /// Whether the lease names nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.access_key_id.is_none()
            && self.secret_access_key.is_none()
            && self.session_token.is_none()
            && self.expire_time.is_none()
    }
}
