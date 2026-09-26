use std::fs;
use std::path::PathBuf;

use pqbench::dump::{put, DumpFile};

fn file(path: &str, uri: PathBuf) -> DumpFile {
    DumpFile {
        path: path.into(),
        uri: uri.to_string_lossy().into_owned(),
        env: Default::default(),
    }
}

fn payload() -> &'static [u8] {
    b"PAR1 a parquet object's bytes PAR1"
}

#[tokio::test]
async fn copies_a_local_file_to_the_output_directory() {
    let source = tempfile::tempdir().unwrap();
    let object = source.path().join("part-0.parquet");
    fs::write(&object, payload()).unwrap();
    let output = tempfile::tempdir().unwrap();

    let summary = put(&[file("year=2024/part-0.parquet", object)], output.path())
        .await
        .unwrap();

    assert_eq!(summary.file_count, 1);
    assert_eq!(summary.byte_count, payload().len() as u64);
    let copied = output.path().join("year=2024/part-0.parquet");
    assert_eq!(fs::read(copied).unwrap(), payload());
}

#[tokio::test]
async fn refuses_a_path_that_escapes_the_output_directory() {
    let source = tempfile::tempdir().unwrap();
    let object = source.path().join("part-0.parquet");
    fs::write(&object, payload()).unwrap();
    let output = tempfile::tempdir().unwrap();

    let error = put(&[file("../escape.parquet", object)], output.path())
        .await
        .unwrap_err();

    assert!(error.to_string().contains("refusing"), "{error}");
    assert!(!output
        .path()
        .parent()
        .unwrap()
        .join("escape.parquet")
        .exists());
}
