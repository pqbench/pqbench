use serde::{Deserialize, Serialize};

/// An on-disk table format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum TableFormat {
    #[serde(rename = "TABLE_FORMAT_DELTA")]
    Delta,
    #[serde(rename = "TABLE_FORMAT_ICEBERG")]
    Iceberg,
}

impl TableFormat {
    /// The proto value name, as the wire spells it.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Delta => "TABLE_FORMAT_DELTA",
            Self::Iceberg => "TABLE_FORMAT_ICEBERG",
        }
    }
}
