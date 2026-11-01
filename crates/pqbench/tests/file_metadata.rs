use crate::common;

use std::sync::Arc;

use parquet::data_type::Int32Type;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use pqbench::v1;

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
            let blob = v1::name::blob_name(&input);
            let mut blob = pqbench::blob::get(common::lake(), &blob).await.unwrap();
            let columns = common::columns(&input).await;
            blob.row_group_count = Some(
                columns
                    .first()
                    .and_then(|column| column.row_group_count)
                    .unwrap_or(0),
            );
            blob.column_count = Some(columns.len() as u64);
            assert_eq!(blob.size_bytes, Some(size_bytes));
            assert_eq!(blob.row_group_count, Some(if populated { 2 } else { 0 }));
            assert_eq!(blob.column_count, Some(columns.len() as u64));
            if populated {
                assert_eq!(columns.len(), 1);
                assert_eq!(
                    columns[0].mass.as_ref().and_then(|mass| mass.value_count),
                    Some(3)
                );
            } else {
                assert!(columns.is_empty());
            }
            snapshots.push(serde_json::to_value(&blob).unwrap());
        }
        assert_eq!(snapshots[0], snapshots[1]);
    }
}
