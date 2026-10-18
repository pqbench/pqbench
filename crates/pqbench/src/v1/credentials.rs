use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Why a table is not readable outside Databricks compute.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum EligibilityReason {
    #[serde(rename = "ELIGIBILITY_REASON_SYSTEM_TABLE")]
    SystemTable,
    #[serde(rename = "ELIGIBILITY_REASON_NOT_A_TABLE")]
    NotATable,
    #[serde(rename = "ELIGIBILITY_REASON_NO_EXTERNAL_READ")]
    NoExternalRead,
}

impl EligibilityReason {
    /// The proto value name, as the wire spells it.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SystemTable => "ELIGIBILITY_REASON_SYSTEM_TABLE",
            Self::NotATable => "ELIGIBILITY_REASON_NOT_A_TABLE",
            Self::NoExternalRead => "ELIGIBILITY_REASON_NO_EXTERNAL_READ",
        }
    }
}

/// Vended storage credentials. Non-sensitive for now, so a [`Table`] may carry
/// them; a later pass moves them off the wire.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
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
    /// When the lease expires, RFC-3339.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expire_time: Option<String>,
}

impl Credentials {
    /// The lease the `AWS_*` options a document carries name.
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

/// The result `credentials check` returns.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ReadEligibility {
    /// Whether the table is readable outside Databricks compute.
    #[serde(default)]
    pub eligible: bool,
    /// Why not, when it is not.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<EligibilityReason>,
}
