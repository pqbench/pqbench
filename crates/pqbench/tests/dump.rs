use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use parquet::data_type::Int64Type;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use pqbench::dump::{self, DumpFile, DumpRequest, RowGroups};

fn parquet_fixture() -> String {
    format!(
        "{}/tests/fixtures/small_reddit_none.parquet",
        env!("CARGO_MANIFEST_DIR")
    )
}

fn request(files: Vec<DumpFile>, row_groups: RowGroups) -> DumpRequest {
    DumpRequest { files, row_groups }
}

fn file(path: &str, uri: String) -> DumpFile {
    DumpFile {
        path: path.into(),
        uri,
        table: None,
        env: Default::default(),
    }
}

fn write_groups(path: &Path, groups: &[i64]) {
    let schema = Arc::new(parse_message_type("message data { REQUIRED INT64 id; }").unwrap());
    let mut writer =
        SerializedFileWriter::new(File::create(path).unwrap(), schema, Default::default()).unwrap();
    let mut next = 0i64;
    for rows in groups {
        let mut group = writer.next_row_group().unwrap();
        let mut column = group.next_column().unwrap().unwrap();
        let values: Vec<_> = (next..next + *rows).collect();
        next += *rows;
        column
            .typed::<Int64Type>()
            .write_batch(&values, None, None)
            .unwrap();
        column.close().unwrap();
        group.close().unwrap();
    }
    writer.close().unwrap();
}

#[tokio::test]
async fn dump_reads_rows_from_a_local_file() {
    let dump = dump::dump(&request(
        vec![file("reddit.parquet", parquet_fixture())],
        RowGroups::ALL,
    ))
    .await
    .unwrap();
    assert_eq!(dump.columns[0], "_path");
    assert!(dump.columns.iter().any(|column| column == "url_encoded"));
    assert_eq!(dump.rows.len(), 3000);
    assert_eq!(dump.rows[0][0], "reddit.parquet");
    let csv = dump::render_csv(&dump);
    assert!(csv.starts_with("_path,"));
    assert!(csv.contains("url_encoded"));
    let json = dump::render_json(&dump).unwrap();
    let first: serde_json::Value = serde_json::from_str(json.lines().next().unwrap()).unwrap();
    assert_eq!(first["_path"], "reddit.parquet");
    assert!(first.get("url_encoded").is_some());
}

#[tokio::test]
async fn dump_rejects_empty_files() {
    let error = dump::dump(&request(Vec::new(), RowGroups::ALL))
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("no files"), "{error}");
}

#[tokio::test]
async fn dump_reads_the_first_row_groups() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("groups.parquet");
    write_groups(&path, &[4, 6]);
    let uri = path.to_string_lossy().into_owned();

    let all = dump::dump(&request(
        vec![file("groups.parquet", uri.clone())],
        RowGroups::ALL,
    ))
    .await
    .unwrap();
    assert_eq!(all.rows.len(), 10);

    let first = dump::dump(&request(
        vec![file("groups.parquet", uri.clone())],
        RowGroups::First(1),
    ))
    .await
    .unwrap();
    assert_eq!(first.rows.len(), 4);
    assert_eq!(first.rows[0][1], 0);
    assert_eq!(first.rows[3][1], 3);

    let parquet = dump::write_parquet(&request(
        vec![file("groups.parquet", uri)],
        RowGroups::First(1),
    ))
    .await
    .unwrap();
    assert!(parquet.starts_with(b"PAR1"));
    let reader = SerializedFileReader::new(bytes::Bytes::from(parquet)).unwrap();
    assert_eq!(reader.num_row_groups(), 1);
    assert_eq!(reader.metadata().file_metadata().num_rows(), 4);
}

#[test]
fn row_groups_parses_all_or_first() {
    assert_eq!(RowGroups::parse("all").unwrap(), RowGroups::ALL);
    assert_eq!(RowGroups::parse("first:3").unwrap(), RowGroups::First(3));
    let error = RowGroups::parse("every:2").unwrap_err().to_string();
    assert!(error.contains("first:N"), "{error}");
    assert!(RowGroups::parse("first:0").is_err());
}
