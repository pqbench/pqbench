//! `writer`: the writer a file came from, and the knobs it has.

/// The writer that produced a file, as the footer's `created_by` says.
///
/// A suggestion's remedy names a setting, so it has to be a setting the
/// writing application actually has: parquet-mr takes per-column keys,
/// pyarrow and parquet-rs take per-column writer options, and Photon exposes
/// no per-column setting at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Writer {
    /// Databricks Photon.
    Photon,
    /// Apache parquet-mr (Spark, Hadoop, Hive).
    ParquetMr,
    /// Apache Arrow C++ (pyarrow).
    ArrowCpp,
    /// Apache Arrow Rust (parquet-rs).
    ArrowRs,
    /// A writer this crate does not classify.
    Other,
}

impl Writer {
    /// Classify a footer's `created_by`.
    pub(crate) fn classify(name: Option<&str>) -> Self {
        let rendered = name.unwrap_or_default().to_lowercase();
        if rendered.contains("photon") {
            Self::Photon
        } else if rendered.contains("parquet-mr") || rendered.contains("parquet-java") {
            Self::ParquetMr
        } else if rendered.contains("parquet-cpp") {
            Self::ArrowCpp
        } else if rendered.contains("parquet-rs") {
            Self::ArrowRs
        } else {
            Self::Other
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Writer;

    #[test]
    fn real_footer_names_are_classified() {
        assert_eq!(
            Writer::classify(Some(
                "parquet-mr compatible Photon version 0.2 (build 18.3)"
            )),
            Writer::Photon
        );
        assert_eq!(
            Writer::classify(Some("parquet-mr version 1.13.1 (build db41831)")),
            Writer::ParquetMr
        );
        assert_eq!(
            Writer::classify(Some("parquet-cpp-arrow version 21.0.0")),
            Writer::ArrowCpp
        );
        assert_eq!(
            Writer::classify(Some("parquet-rs version 57.3.0")),
            Writer::ArrowRs
        );
        assert_eq!(Writer::classify(Some("some other writer")), Writer::Other);
        assert_eq!(Writer::classify(None), Writer::Other);
    }
}
