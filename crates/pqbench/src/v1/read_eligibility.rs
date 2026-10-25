use serde::{Deserialize, Serialize};

use super::EligibilityReason;

/// The result `credentials check` returns.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ReadEligibility {
    /// Whether the table is readable outside Databricks compute.
    #[serde(default)]
    pub eligible: bool,
    /// Why not, when it is not.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<EligibilityReason>,
}
