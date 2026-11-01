#![cfg(feature = "aws")]

//! Blackbox end-to-end test of the public `bytemass` command over S3.
//!
//! Dataset: [Amazon Redset](https://github.com/amazon-science/redset)
//! (Amazon.com, Inc. and affiliates), serverless `sample_0.001.parquet`,
//! licensed [CC BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/).
//!
//! Ignored by default because it reads a public S3 object; run with `make e2e`.

use pqbench::v1;

mod common;

const REDSET_SAMPLE_0_001: &str = "s3://redshift-downloads/redset/serverless/sample_0.001.parquet";

#[tokio::test]
#[ignore = "network: reads a public object from s3://redshift-downloads"]
async fn measures_the_redset_sample_anonymously() {
    std::env::set_var("AWS_SKIP_SIGNATURE", "true");
    let blob = v1::name::blob_name(REDSET_SAMPLE_0_001);
    let columns = pqbench::bytemass::list_columns(common::lake(), &blob)
        .await
        .unwrap();

    assert!(!columns.is_empty());
    let row_count: u64 = columns
        .iter()
        .map(|column| {
            column
                .mass
                .as_ref()
                .and_then(|mass| mass.value_count)
                .unwrap_or(0)
        })
        .sum();
    assert!(row_count > 0);
    for column in &columns {
        let (service, locator, _) = v1::Column::parse_name(&column.name).expect("a column name");
        assert_eq!(v1::name::blob(&service, &locator), blob);
    }
}
