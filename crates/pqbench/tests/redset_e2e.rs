#![cfg(feature = "aws")]

//! Blackbox end-to-end test of the public `bytemass` command over S3.
//!
//! Dataset: [Amazon Redset](https://github.com/amazon-science/redset)
//! (Amazon.com, Inc. and affiliates), serverless `sample_0.001.parquet`,
//! licensed [CC BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/).
//!
//! Ignored by default because it reads a public S3 object; run with `make e2e`.

use pqbench::bytemass::{bytemass, BytemassRequest};

const REDSET_SAMPLE_0_001: &str = "s3://redshift-downloads/redset/serverless/sample_0.001.parquet";

#[test]
#[ignore = "network: reads a public object from s3://redshift-downloads"]
fn measures_the_redset_sample_anonymously() {
    std::env::set_var("AWS_SKIP_SIGNATURE", "true");
    let request = BytemassRequest {
        inputs: vec![REDSET_SAMPLE_0_001.to_string()],
        ..BytemassRequest::default()
    };
    let output = bytemass(&request).unwrap();

    let summary = output.summary.as_ref().unwrap();
    assert_eq!(summary.file_count, 1);
    assert!(summary.num_rows > 0);
    assert!(!summary.columns.is_empty());

    let files = output.files.as_ref().unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].path, REDSET_SAMPLE_0_001);
    assert_eq!(files[0].mass.num_rows, summary.num_rows);
    assert_eq!(files[0].mass.columns.len(), summary.columns.len());

    print!("{output}");
}
