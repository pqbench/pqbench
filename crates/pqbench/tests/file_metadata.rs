use std::sync::Arc;

use parquet::data_type::Int32Type;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use pqbench::bytemass::{measure_files, BytemassRequest};

#[tokio::test]
async fn footer_facts_survive_empty_files_and_match_file_uri_reads() {
    for populated in [false, true] {
        let file = tempfile::NamedTempFile::new().unwrap();
        let properties = WriterProperties::builder()
            .set_created_by("metadata-test".into())
            .set_key_value_metadata(Some(vec![
                KeyValue::new("same".into(), Some("é".repeat(200))),
                KeyValue::new("same".into(), None),
                KeyValue::new("empty".into(), Some(String::new())),
            ]))
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
        let mut snapshots = Vec::new();
        for input in [
            file.path().to_str().unwrap().to_owned(),
            url::Url::from_file_path(file.path()).unwrap().to_string(),
        ] {
            let result = measure_files(&BytemassRequest {
                inputs: vec![input],
                ..Default::default()
            })
            .await
            .unwrap();
            assert_eq!(result.len(), 1);
            let result = &result[0];
            let metadata = result.file.metadata.as_ref().unwrap();
            assert_eq!(metadata.creator.as_deref(), Some("metadata-test"));
            assert_eq!(metadata.format_version, 1);
            assert_eq!(metadata.key_values.len(), 3);
            assert_eq!(
                metadata.key_values[0].value.as_deref(),
                Some("é".repeat(128).as_str())
            );
            assert_eq!(metadata.key_values[0].value_bytes, Some(400));
            assert!(metadata.key_values[0].truncated);
            assert!(metadata.key_values[1].value.is_none());
            assert_eq!(metadata.key_values[2].value.as_deref(), Some(""));
            assert_eq!(result.row_count, if populated { 3 } else { 0 });
            assert_eq!(metadata.row_groups.len(), if populated { 2 } else { 0 });
            if populated {
                assert_eq!(metadata.row_groups[0].row_count, 2);
                assert_eq!(metadata.row_groups[1].row_count, 1);
                assert_eq!(
                    metadata.row_groups[0].compressed_bytes,
                    result.columns[0].compressed_bytes
                );
                assert_eq!(
                    metadata.row_groups[0].uncompressed_bytes,
                    result.columns[0].uncompressed_bytes
                );
                assert_eq!(metadata.row_groups[0].offset_indexes, vec![true]);
            }
            snapshots.push(serde_json::to_value(metadata).unwrap());
        }
        assert_eq!(snapshots[0], snapshots[1]);
    }
}
