use serde::{Deserialize, Serialize};

/// One frequent value and how many times it occurred.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValueCount {
    pub value: String,
    pub count: u64,
}
