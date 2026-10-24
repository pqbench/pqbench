mod common;

use std::sync::Arc;

use parquet::data_type::Int32Type;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use pqbench::bytemass::{measure_files, BytemassRequest};

#[tokio::test]
async fn a_bare_path_and_a_file_uri_measure_the_same_blob_and_columns() {
    for populated in [false, true] {
        let file = tempfile::NamedTempFile::new().unwrap();
        let properties = WriterProperties::builder()
            .set_created_by("metadata-test".into())
            .build();
        let schema = Arc::new(parse_message_type("message test { REQUIRED INT32 id; }").unwrap());
        let mut writer =
            SerializedFileWriter::new(file.reopen().unwrap(), schema, Arc::new(properties))
                .unwrap();
        if populated {
            for values in [&[1, 2][..], &[3][..]] {
                let mut group = writer.next_row_group().unwrap();
                let mut column = group.next_column().unwrap().unwrap();
                column
                    .typed::<Int32Type>()
                    .write_batch(values, None, None)
                    .unwrap();
                column.close().unwrap();
                group.close().unwrap();
            }
        }
        writer.close().unwrap();
        let size_bytes = std::fs::metadata(file.path()).unwrap().len();

        let mut snapshots = Vec::new();
        for input in [
            file.path().to_str().unwrap().to_owned(),
            url::Url::from_file_path(file.path()).unwrap().to_string(),
        ] {
            let result = measure_files(
                common::lake(),
                &BytemassRequest {
                    inputs: vec![input],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            assert_eq!(result.len(), 1);
            let measured = &result[0];
            assert_eq!(measured.blob.size_bytes, Some(size_bytes));
            assert_eq!(
                measured.blob.row_group_count,
                Some(if populated { 2 } else { 0 })
            );
            assert_eq!(
                measured.blob.column_count,
                Some(measured.columns.len() as u64)
            );
            if populated {
                assert_eq!(measured.columns.len(), 1);
                assert_eq!(measured.columns[0].value_count, Some(3));
            } else {
                assert!(measured.columns.is_empty());
            }
            snapshots.push(serde_json::to_value(&measured.blob).unwrap());
        }
        assert_eq!(snapshots[0], snapshots[1]);
    }
}
