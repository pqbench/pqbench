use serde::{Deserialize, Serialize};

/// One candidate monthly storage cost: a class with one price, or one access
/// sub-tier of a class that chooses its price by access.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StorageTierCost {
    /// The storage class, e.g. `STANDARD` or `INTELLIGENT_TIERING`.
    pub storage_class: String,
    /// The access sub-tier, for a class that prices by access.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub access_tier: Option<String>,
    /// The S3 list price this tier charges, in dollars per gibibyte per month.
    pub dollars_per_gibibyte_per_month: f64,
    /// The estimated monthly cost at this tier of what was priced.
    pub cost_per_month: f64,
}
