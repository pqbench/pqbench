use serde::{Deserialize, Serialize};

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
