#![cfg(feature = "aws")]

//! Blackbox end-to-end test of the public `bytemass` command over S3.
//!
//! Dataset: [Amazon Redset](https://github.com/amazon-science/redset)
//! (Amazon.com, Inc. and affiliates), serverless `sample_0.001.parquet`,
//! licensed [CC BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/).
//!
//! Ignored by default because it reads a public S3 object; run with `make e2e`.

use pqbench::bytemass::{measure_files, BytemassRequest};
use pqbench::v1;

mod common;

const REDSET_SAMPLE_0_001: &str = "s3://redshift-downloads/redset/serverless/sample_0.001.parquet";

#[tokio::test]
#[ignore = "network: reads a public object from s3://redshift-downloads"]
async fn measures_the_redset_sample_anonymously() {
    std::env::set_var("AWS_SKIP_SIGNATURE", "true");
    let request = BytemassRequest {
        inputs: vec![REDSET_SAMPLE_0_001.to_string()],
        ..Default::default()
    };
    let files = measure_files(common::lake(), &request).await.unwrap();

    assert_eq!(files.len(), 1);
    let file = &files[0];
    assert_eq!(file.blob.name, v1::name::blob_name(REDSET_SAMPLE_0_001));
    assert!(!file.columns.is_empty());
    let row_count: u64 = file
        .columns
        .iter()
        .map(|column| column.value_count.unwrap_or(0))
        .sum();
    assert!(row_count > 0);
    for column in &file.columns {
        let (blob, _) = v1::name::parse_bytemass(&column.name).expect("a bytemass name");
        assert_eq!(blob, file.blob.name);
    }
}
