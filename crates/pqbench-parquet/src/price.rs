//! Prices as data: the S3 list price of each storage class.

/// Whether a blob service is S3 object storage (the Hadoop-prefixed `s3a` too).
pub fn object_storage(service: &str) -> bool {
    matches!(service, "s3" | "s3a")
}

/// Bytes in a gibibyte. The list prices are quoted per GB-month; this uses the
/// binary gigabyte AWS bills against.
pub const GIBIBYTE: f64 = 1_073_741_824.0;

/// The S3 list prices per GiB-month (us-east-1) for a storage class, keyed by
/// access sub-tier: one entry for a class with a single price, five for
/// Intelligent-Tiering.
///
/// HEAD reports only the class, never the sub-tier, so a class that prices by
/// access returns every sub-tier (Frequent Access down to Deep Archive Access)
/// and the consumer picks. The sub-tier is chosen by how often the object is
/// read; the object monitoring fee is not included.
pub fn storage_prices(storage_class: &str) -> Option<&'static [(Option<&'static str>, f64)]> {
    Some(match storage_class {
        "STANDARD" => &[(None, 0.023)],
        "REDUCED_REDUNDANCY" => &[(None, 0.024)],
        "INTELLIGENT_TIERING" => &[
            (Some("FREQUENT_ACCESS"), 0.023),
            (Some("INFREQUENT_ACCESS"), 0.0125),
            (Some("ARCHIVE_INSTANT_ACCESS"), 0.004),
            (Some("ARCHIVE_ACCESS"), 0.0036),
            (Some("DEEP_ARCHIVE_ACCESS"), 0.00099),
        ],
        "STANDARD_IA" => &[(None, 0.0125)],
        "ONEZONE_IA" => &[(None, 0.01)],
        "EXPRESS_ONEZONE" => &[(None, 0.11)],
        "GLACIER_IR" => &[(None, 0.004)],
        "GLACIER" => &[(None, 0.0036)],
        "DEEP_ARCHIVE" => &[(None, 0.00099)],
        _ => return None,
    })
}
