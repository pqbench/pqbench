use parquet::basic::Compression;
use parquet::data_type::Int32Type;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use pqbench::bytemass::scan_pages;
use pqbench::third_party::parquet::api::{read_file_masses, read_page_header};
use std::sync::Arc;

#[tokio::test]
async fn scans_compressed_v1_and_v2_pages_without_indexes_or_payload_decoding() {
    for version in [WriterVersion::PARQUET_1_0, WriterVersion::PARQUET_2_0] {
        let file = tempfile::NamedTempFile::new().unwrap();
        let properties = WriterProperties::builder()
            .set_writer_version(version)
            .set_compression(Compression::SNAPPY)
            .set_dictionary_enabled(true)
            .set_offset_index_disabled(true)
            .set_statistics_enabled(EnabledStatistics::None)
            .set_data_page_row_count_limit(2)
            .set_write_batch_size(2)
            .build();
        let schema = Arc::new(parse_message_type("message test { REQUIRED INT32 id; }").unwrap());
        let mut writer =
            SerializedFileWriter::new(file.reopen().unwrap(), schema, Arc::new(properties))
                .unwrap();
        let mut group = writer.next_row_group().unwrap();
        let mut column = group.next_column().unwrap().unwrap();
        column
            .typed::<Int32Type>()
            .write_batch(&[1, 2, 1, 2, 1, 2], None, None)
            .unwrap();
        column.close().unwrap();
        group.close().unwrap();
        writer.close().unwrap();
        let mass = read_file_masses(file.path(), true).unwrap();
        assert!(mass.columns[0].page_count.is_none());
        let pages = scan_pages(file.path().to_str().unwrap(), &Default::default())
            .await
            .unwrap();
        assert_eq!(pages[0].header.page_type, "DICTIONARY_PAGE");
        assert_eq!(pages[0].header.dictionary_entries, Some(2));
        assert_eq!(pages[0].header.encoding.as_deref(), Some("PLAIN"));
        let data: Vec<_> = pages
            .iter()
            .filter(|p| p.header.value_count.is_some())
            .collect();
        assert_eq!(data.len(), 3);
        assert_eq!(
            data.iter()
                .map(|p| p.header.value_count.unwrap())
                .sum::<u64>(),
            6
        );
        for page in data {
            assert_eq!(
                page.header.page_type,
                if version == WriterVersion::PARQUET_1_0 {
                    "DATA_PAGE"
                } else {
                    "DATA_PAGE_V2"
                }
            );
            assert!(matches!(
                page.header.encoding.as_deref(),
                Some("PLAIN_DICTIONARY" | "RLE_DICTIONARY")
            ));
        }
        assert_eq!(
            pages
                .iter()
                .map(|p| p.header.header_bytes + p.header.compressed_bytes)
                .sum::<u64>(),
            mass.columns[0].compressed_bytes
        );
        let mut bytes = std::fs::read(file.path()).unwrap();
        for page in &pages {
            let start = (page.offset + page.header.header_bytes) as usize;
            let end = start + page.header.compressed_bytes as usize;
            bytes[start..end].fill(0xff);
        }
        std::fs::write(file.path(), bytes).unwrap();
        let corrupted = scan_pages(file.path().to_str().unwrap(), &Default::default())
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_value(&pages).unwrap(),
            serde_json::to_value(corrupted).unwrap()
        );
    }
}

#[test]
fn rejects_incomplete_and_negative_headers() {
    assert!(read_page_header(&[]).is_err());
    // Compact Thrift: DATA_PAGE type, uncompressed size -1, compressed size 0.
    assert!(read_page_header(&[0x15, 0, 0x15, 1, 0x15, 0, 0]).is_err());
}
