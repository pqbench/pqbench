#![cfg(feature = "delta")]

mod common;
mod support;

use futures_util::TryStreamExt;
use pqbench::table::{self, LoadRequest};
use pqbench::third_party::parquet::api::{default_metadata_parser, MetadataParser};
use pqbench::v1::TableFormat;
use serde_json::json;
use support::{metadata, remove, write_parquet, Fixture};

fn load_request(uri: impl Into<String>, version: Option<u64>) -> LoadRequest {
    LoadRequest::new(uri, version, Default::default())
}

#[tokio::test]
async fn load_emits_every_json_commit_and_only_active_files() {
    let fixture = Fixture::new();
    let info = table::load(
        common::lake(),
        &load_request(fixture.path().to_string_lossy(), None),
    )
    .await
    .unwrap();
    assert_eq!(info.format, TableFormat::Delta);
    assert_eq!(info.snapshot_version, 1);
    assert_eq!(info.log.len(), 2);
    assert!(info.log[1]
        .actions
        .iter()
        .any(|action| action.kind.as_deref() == Some("remove")));
    let mut paths: Vec<_> = info.files.iter().map(|file| file.path.as_str()).collect();
    paths.sort_unstable();
    assert_eq!(paths, ["part=a/added.parquet", "part=b/kept.parquet"]);
    let files = pqbench::bytemass::measure_files(
        common::lake(),
        &pqbench::bytemass::BytemassRequest {
            inputs: info.files.iter().map(|file| file.uri().unwrap()).collect(),
            env: info.env.clone(),
        },
    )
    .await
    .unwrap();
    assert_eq!(files.len(), 2);
    let row_count: u64 = files
        .iter()
        .flat_map(|file| &file.columns)
        .map(|column| column.value_count.unwrap_or(0))
        .sum();
    assert_eq!(row_count, 14);
}

/// Spark writes a bare `NULL` as a `void` column. Delta leaves the type
/// undefined, so a reader must not fail resolving the snapshot: the table loads
/// its files and keeps the column in the declared schema.
#[tokio::test]
async fn load_reads_a_void_column() {
    let directory = tempfile::tempdir().unwrap();
    let log = directory.path().join("_delta_log");
    std::fs::create_dir(&log).unwrap();
    write_parquet(&directory.path().join("data.parquet"), 1);
    let size = std::fs::metadata(directory.path().join("data.parquet"))
        .unwrap()
        .len();
    let schema = json!({
        "type": "struct",
        "fields": [
            {"name": "id", "type": "long", "nullable": true, "metadata": {}},
            {"name": "note", "type": "void", "nullable": true, "metadata": {}}
        ]
    });
    let commit = format!(
        "{}\n{}\n{}\n",
        json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}),
        json!({"metaData": {
            "id": "5b0f6a3e-2f7d-4f43-9a55-8f0c0c1d2e3f",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": schema.to_string(),
            "partitionColumns": [],
            "configuration": {},
            "createdTime": 1_760_000_000_000i64
        }}),
        json!({"add": {
            "path": "data.parquet",
            "size": size,
            "partitionValues": {},
            "modificationTime": 1_760_000_000_000i64,
            "dataChange": true
        }})
    );
    std::fs::write(log.join("00000000000000000000.json"), commit).unwrap();

    let info = table::load(
        common::lake(),
        &load_request(directory.path().to_string_lossy(), None),
    )
    .await
    .unwrap();
    assert_eq!(info.format, TableFormat::Delta);
    assert_eq!(info.files.len(), 1);
    let columns: Vec<_> = info
        .columns
        .iter()
        .map(|column| (column.column.as_str(), column.data_type.as_deref()))
        .collect();
    assert_eq!(columns, [("id", Some("long")), ("note", Some("void"))]);
}

/// `list partitions` streams the log for commit times: each commit arrives with
/// its version, its time and the `commitInfo` the log stored, and no action, so
/// nothing grows with a commit's file list. A commit the log does not date is
/// not visited.
#[tokio::test]
async fn log_only_read_streams_times_without_actions() {
    let fixture = Fixture::new();
    let base = 1_700_000_000_000i64;
    fixture.commit(
        2,
        &[json!({"commitInfo": {"timestamp": base, "operation": "WRITE"}})],
    );
    fixture.commit(3, &[json!({"commitInfo": {"timestamp": base + 1_000}})]);

    let request = load_request(fixture.path().to_string_lossy(), None).without_files();
    let mut seen: Vec<(u64, Option<i64>, Option<String>, usize)> = Vec::new();
    let mut commits = table::stream_commits(common::lake(), &request)
        .await
        .unwrap();
    while let Some(commit) = commits.try_next().await.unwrap() {
        let operation = commit
            .commit_info
            .as_ref()
            .and_then(|info| info.get("operation"))
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned);
        seen.push((
            commit.version,
            commit.commit_time,
            operation,
            commit.actions.len(),
        ));
    }

    // The walk is newest first, and the newest commit is the snapshot version.
    assert_eq!(
        seen.iter()
            .map(|(version, _, _, _)| *version)
            .collect::<Vec<_>>(),
        [3, 2]
    );
    assert_eq!(seen[0].1, Some(base + 1_000));
    assert_eq!(seen[0].2, None, "a commitInfo that records no operation");
    assert_eq!(seen[1].1, Some(base));
    assert_eq!(seen[1].2.as_deref(), Some("WRITE"));
    assert!(seen.iter().all(|(_, _, _, actions)| *actions == 0));
}

/// A commit whose `commitInfo` is not the first action is still dated: the walk
/// reads past the header when the log does not lead with it.
#[tokio::test]
async fn log_only_read_dates_a_header_that_is_not_first() {
    let fixture = Fixture::new();
    let base = 1_700_000_000_000i64;
    fixture.commit(
        2,
        &[
            // Longer than the header the walk reads first.
            json!({"add": {
                "path": "padded.parquet",
                "partitionValues": {"part": "a"},
                "size": 1,
                "modificationTime": 0,
                "dataChange": true,
                "stats": "x".repeat(8_192),
            }}),
            json!({"commitInfo": {"timestamp": base}}),
        ],
    );

    let request = load_request(fixture.path().to_string_lossy(), None).without_files();
    let mut commits = table::stream_commits(common::lake(), &request)
        .await
        .unwrap();
    let mut seen = Vec::new();
    while let Some(commit) = commits.try_next().await.unwrap() {
        seen.push((commit.version, commit.commit_time));
    }

    assert_eq!(seen, [(2, Some(base))]);
}

/// `list files` streams the named commits' `add` actions — the files those
/// commits wrote, in version order, without a snapshot resolved. A version the
/// log does not hold has nothing to contribute.
#[tokio::test]
async fn file_stream_names_the_adds_of_the_named_commits() {
    let fixture = Fixture::new();
    // The fixture's version 0 adds two files and version 1 removes one and adds
    // another; only what a commit added is named, never what it removed.
    let first = file_uris(&fixture, &[0]).await;
    assert_eq!(first.len(), 2);
    assert!(first[0].ends_with("part=a/old%20file.parquet"), "{first:?}");
    assert!(first[1].ends_with("part=b/kept.parquet"), "{first:?}");
    let second = file_uris(&fixture, &[1]).await;
    assert_eq!(second.len(), 1);
    assert!(second[0].ends_with("part=a/added.parquet"), "{second:?}");

    // Log order is version order, and 7 is not in the log.
    let both = file_uris(&fixture, &[0, 1, 7]).await;
    assert_eq!(both, [first, second].concat());
    assert!(file_uris(&fixture, &[]).await.is_empty());
}

/// A log that holds `add` actions and nothing else still names its files: the
/// read is the commits' own actions, so it resolves no snapshot and needs no
/// protocol or metadata to do it.
#[tokio::test]
async fn file_stream_reads_a_log_without_a_snapshot() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::create_dir(directory.path().join("_delta_log")).unwrap();
    std::fs::write(
        directory
            .path()
            .join("_delta_log/00000000000000000000.json"),
        concat!(
            "{\"add\":{\"path\":\"one.parquet\",\"size\":1,",
            "\"partitionValues\":{},\"modificationTime\":0,\"dataChange\":true}}\n"
        ),
    )
    .unwrap();
    let uris = file_uris_path(&directory.path().to_string_lossy(), &[0]).await;
    assert_eq!(uris.len(), 1);
    assert!(uris[0].ends_with("one.parquet"), "{uris:?}");
}

/// A request that names no commits reads the log's own listing, as
/// `list partitions` names them.
#[tokio::test]
async fn file_stream_reads_every_commit_when_none_are_named() {
    let fixture = Fixture::new();
    let request = load_request(fixture.path().to_string_lossy(), None);
    let files = table::stream_files(common::lake(), &request)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    // Version 0 added two files and version 1 one more.
    assert_eq!(files.len(), 3);
}

/// The stream carries the size the log declares, so `list files` can filter on
/// it before reading any data file.
#[tokio::test]
async fn file_stream_carries_the_log_size() {
    let fixture = Fixture::new();
    let request = load_request(fixture.path().to_string_lossy(), None);
    let files = table::stream_files(common::lake(), &request)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    for path in ["part=a/added.parquet", "part=b/kept.parquet"] {
        let file = files.iter().find(|file| file.path == path).unwrap();
        let expected = std::fs::metadata(fixture.path().join(path)).unwrap().len();
        assert_eq!(file.size_bytes, Some(expected), "{path}");
    }
}

async fn file_uris(fixture: &Fixture, versions: &[u64]) -> Vec<String> {
    file_uris_path(&fixture.path().to_string_lossy(), versions).await
}

async fn file_uris_path(uri: &str, versions: &[u64]) -> Vec<String> {
    let request = load_request(uri, None).with_commits(versions.iter().copied());
    let mut files = table::stream_files(common::lake(), &request).await.unwrap();
    let mut uris = Vec::new();
    while let Some(file) = files.try_next().await.unwrap() {
        uris.push(file.uri().unwrap());
    }
    uris
}

#[tokio::test]
async fn load_selects_a_snapshot_and_bytemass_weights_columns() {
    let fixture = Fixture::new();
    let previous = table::load(
        common::lake(),
        &load_request(fixture.path().to_string_lossy(), Some(0)),
    )
    .await
    .unwrap();
    assert_eq!(previous.snapshot_version, 0);
    assert_eq!(previous.files.len(), 2);

    let latest = table::load(
        common::lake(),
        &load_request(fixture.path().to_string_lossy(), None),
    )
    .await
    .unwrap();
    assert_eq!(latest.snapshot_version, 1);
    assert_eq!(latest.files.len(), 2);
    assert_eq!(latest.partition_columns, ["part"]);
    let added = latest
        .files
        .iter()
        .find(|file| file.path == "part=a/added.parquet")
        .unwrap();
    let stats = added.stats.as_ref().unwrap();
    assert_eq!(stats.record_count, 9);
    assert_eq!(stats.first_values["id"], 0);
    assert_eq!(stats.last_values["id"], 8);
    assert_eq!(stats.null_count["id"], 0);
    assert_eq!(stats.tight_bounds, Some(true));
    assert_eq!(
        stats.bytes_per_row,
        Some(added.size_bytes.unwrap_or(0) as f64 / stats.record_count as f64)
    );
    assert_eq!(added.partition_values["part"].as_deref(), Some("a"));
    let part_a = latest
        .partitions
        .iter()
        .find(|partition| partition.values.get("part").map(Option::as_deref) == Some(Some("a")))
        .unwrap();
    assert_eq!(part_a.file_count, 1);
    assert_eq!(part_a.num_records, Some(9));
    assert_eq!(part_a.bytes_per_row, stats.bytes_per_row);
    assert_eq!(Some(part_a.size), added.size_bytes);

    let files = pqbench::bytemass::measure_files(
        common::lake(),
        &pqbench::bytemass::BytemassRequest {
            inputs: latest
                .files
                .iter()
                .map(|file| file.uri().unwrap())
                .collect(),
            env: latest.env.clone(),
        },
    )
    .await
    .unwrap();
    assert_eq!(files.len(), 2);
    let mut measured_bytes = 0;
    let mut measured_uncompressed = 0;
    let mut measured_values = 0;
    for file in &files {
        assert_eq!(file.columns.len(), 1);
        let (_, column) =
            pqbench::v1::name::parse_bytemass(&file.columns[0].name).expect("a bytemass name");
        assert_eq!(column, "id");
        measured_bytes += file.columns[0].compressed_bytes.unwrap_or(0);
        measured_uncompressed += file.columns[0].uncompressed_bytes.unwrap_or(0);
        measured_values += file.columns[0].value_count.unwrap_or(0);
    }
    assert_eq!(measured_values, 14);
    let mut bytes = 0;
    let mut uncompressed = 0;
    let mut file_bytes = 0;
    for relative in ["part=b/kept.parquet", "part=a/added.parquet"] {
        let path = fixture.path().join(relative);
        let mass = default_metadata_parser().read_masses(&path).unwrap();
        bytes += mass.columns[0].compressed_bytes;
        uncompressed += mass.columns[0].uncompressed_bytes;
        file_bytes += std::fs::metadata(path).unwrap().len();
    }
    assert_eq!(
        latest
            .files
            .iter()
            .map(|file| file.size_bytes.unwrap_or(0))
            .sum::<u64>(),
        file_bytes
    );
    assert_eq!(measured_bytes, bytes);
    assert_eq!(measured_uncompressed, uncompressed);
}

#[tokio::test]
async fn load_accepts_a_file_uri() {
    let fixture = Fixture::new();
    let uri = url::Url::from_directory_path(fixture.path()).unwrap();
    let info = table::load(common::lake(), &load_request(uri.as_str(), None))
        .await
        .unwrap();
    assert_eq!(info.snapshot_version, 1);
    assert_eq!(info.files.len(), 2);
    assert_eq!(info.partition_columns, ["part"]);
}

#[tokio::test]
async fn load_ignores_tombstoned_and_untracked_files() {
    let fixture = Fixture::new();
    std::fs::write(
        fixture.path().join("part=a/old file.parquet"),
        b"not parquet",
    )
    .unwrap();
    std::fs::write(fixture.path().join("untracked.parquet"), b"not parquet").unwrap();
    let latest = table::load(
        common::lake(),
        &load_request(fixture.path().to_string_lossy(), None),
    )
    .await
    .unwrap();
    assert_eq!(latest.files.len(), 2);
    assert!(latest
        .files
        .iter()
        .all(|file| file.path != "part=a/old file.parquet"));
    let files = pqbench::bytemass::measure_files(
        common::lake(),
        &pqbench::bytemass::BytemassRequest {
            inputs: latest
                .files
                .iter()
                .map(|file| file.uri().unwrap())
                .collect(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let row_count: u64 = files
        .iter()
        .flat_map(|file| &file.columns)
        .map(|column| column.value_count.unwrap_or(0))
        .sum();
    assert_eq!(row_count, 14);
    let previous = table::load(
        common::lake(),
        &load_request(fixture.path().to_string_lossy(), Some(0)),
    )
    .await
    .unwrap();
    assert_eq!(previous.files.len(), 2);
}

#[tokio::test]
async fn load_resolves_checkpoint_after_old_json_is_removed() {
    let fixture = Fixture::new();
    fixture.checkpoint().await;
    std::fs::remove_file(fixture.path().join("_delta_log/00000000000000000000.json")).unwrap();
    let info = table::load(
        common::lake(),
        &load_request(fixture.path().to_string_lossy(), Some(1)),
    )
    .await
    .unwrap();
    assert_eq!(info.snapshot_version, 1);
    assert_eq!(info.files.len(), 2);
    let kept = info
        .files
        .iter()
        .find(|file| file.path == "part=b/kept.parquet")
        .unwrap();
    assert_eq!(kept.stats.as_ref().unwrap().record_count, 5);
}

#[tokio::test]
async fn empty_snapshot_names_no_files() {
    let fixture = Fixture::new();
    fixture.commit(
        2,
        &[
            remove("part=b/kept.parquet"),
            remove("part=a/added.parquet"),
        ],
    );
    let info = table::load(
        common::lake(),
        &load_request(fixture.path().to_string_lossy(), None),
    )
    .await
    .unwrap();
    assert_eq!(info.snapshot_version, 2);
    assert!(info.files.is_empty());
}

#[tokio::test]
async fn load_records_log_size_and_bytemass_sees_a_changed_file() {
    let fixture = Fixture::new();
    let path = fixture.path().join("part=a/added.parquet");
    let expected = std::fs::metadata(&path).unwrap().len();
    std::fs::write(&path, b"changed").unwrap();
    let info = table::load(
        common::lake(),
        &load_request(fixture.path().to_string_lossy(), None),
    )
    .await
    .unwrap();
    let added = info
        .files
        .iter()
        .find(|file| file.path == "part=a/added.parquet")
        .unwrap();
    assert_eq!(added.size_bytes, Some(expected));
    let rows = pqbench::bytemass::bytemass(
        common::lake(),
        &pqbench::bytemass::BytemassRequest {
            inputs: vec![added.uri().unwrap()],
            ..Default::default()
        },
    )
    .await;
    assert!(rows.is_err(), "changed file should not measure");
    std::fs::remove_file(path).unwrap();
    let info = table::load(
        common::lake(),
        &load_request(fixture.path().to_string_lossy(), None),
    )
    .await
    .unwrap();
    let added = info
        .files
        .iter()
        .find(|file| file.path == "part=a/added.parquet")
        .unwrap();
    let error = pqbench::bytemass::bytemass(
        common::lake(),
        &pqbench::bytemass::BytemassRequest {
            inputs: vec![added.uri().unwrap()],
            ..Default::default()
        },
    )
    .await
    .err()
    .unwrap();
    let mut message = error.to_string();
    let mut source = std::error::Error::source(&error);
    while let Some(error) = source {
        message.push_str(": ");
        message.push_str(&error.to_string());
        source = error.source();
    }
    assert!(message.contains("No such file or directory"), "{message}");
}

#[tokio::test]
async fn load_rejects_missing_versions_and_non_tables() {
    let fixture = Fixture::new();
    assert!(table::load(
        common::lake(),
        &load_request(fixture.path().to_string_lossy(), Some(99))
    )
    .await
    .is_err());
    let empty = tempfile::tempdir().unwrap();
    let error = table::load(
        common::lake(),
        &load_request(empty.path().to_string_lossy(), None),
    )
    .await
    .err()
    .unwrap()
    .to_string();
    assert!(
        error.contains("unrecognized table format") || error.contains("cannot open table"),
        "{error}"
    );
}

#[tokio::test]
async fn load_does_not_treat_column_mapping_as_byte_mass() {
    let fixture = Fixture::new();
    fixture.commit(2, &[metadata(json!({"delta.columnMapping.mode": "name"}))]);
    match table::load(
        common::lake(),
        &load_request(fixture.path().to_string_lossy(), None),
    )
    .await
    {
        Ok(info) => assert_eq!(info.files.len(), 2),
        Err(error) => assert!(!error.to_string().contains("byte-mass"), "{error}"),
    }
}

#[tokio::test]
async fn load_lists_files_that_carry_deletion_vectors() {
    let fixture = Fixture::new();
    let mut add = fixture.add("part=a/added.parquet", "a", 9);
    add["add"]["deletionVector"] = json!({
        "storageType": "u",
        "pathOrInlineDv": "deletion-vector.bin",
        "offset": 0,
        "sizeInBytes": 1,
        "cardinality": 1
    });
    fixture.commit(2, &[add]);
    let info = table::load(
        common::lake(),
        &load_request(fixture.path().to_string_lossy(), None),
    )
    .await
    .unwrap();
    assert!(info
        .files
        .iter()
        .any(|file| file.path == "part=a/added.parquet"));
}

#[tokio::test]
async fn load_rejects_external_data_paths() {
    let fixture = Fixture::new();
    let parent = fixture.path().parent().unwrap();
    let outside = tempfile::tempdir_in(parent).unwrap();
    write_parquet(&outside.path().join("outside.parquet"), 9);
    let mut add = fixture.add("part=a/added.parquet", "a", 9);
    add["add"]["path"] = json!(format!(
        "../{}/outside.parquet",
        outside.path().file_name().unwrap().to_string_lossy()
    ));
    fixture.commit(2, &[add]);
    let error = table::load(
        common::lake(),
        &load_request(fixture.path().to_string_lossy(), None),
    )
    .await
    .err()
    .unwrap()
    .to_string();
    assert!(error.contains("only relative data paths"), "{error}");
}

#[tokio::test]
async fn load_copies_env_onto_the_document() {
    let fixture = Fixture::new();
    let mut env = std::collections::BTreeMap::new();
    env.insert("AWS_REGION".into(), "us-east-1".into());
    let info = table::load(
        common::lake(),
        &LoadRequest::new(fixture.path().to_string_lossy(), None, env.clone()),
    )
    .await
    .unwrap();
    assert_eq!(info.env, env);
}

#[tokio::test]
async fn load_omits_partition_row_counts_when_a_file_has_no_stats() {
    let fixture = Fixture::new();
    fixture.commit(
        2,
        &[json!({"add": {
            "path": "part=b/nostats.parquet",
            "partitionValues": {"part": "b"},
            "size": 10,
            "modificationTime": 0,
            "dataChange": true
        }})],
    );
    let info = table::load(
        common::lake(),
        &load_request(fixture.path().to_string_lossy(), None),
    )
    .await
    .unwrap();
    let part_b = info
        .partitions
        .iter()
        .find(|partition| partition.values.get("part").map(Option::as_deref) == Some(Some("b")))
        .unwrap();
    assert_eq!(part_b.file_count, 2);
    assert!(part_b.num_records.is_none());
    assert!(part_b.bytes_per_row.is_none());
    assert!(part_b.size > 10);
    let part_a = info
        .partitions
        .iter()
        .find(|partition| partition.values.get("part").map(Option::as_deref) == Some(Some("a")))
        .unwrap();
    assert_eq!(part_a.num_records, Some(9));
}

#[tokio::test]
async fn load_omits_add_stats_that_are_not_json() {
    let fixture = Fixture::new();
    write_parquet(&fixture.path().join("part=a/bad-stats.parquet"), 9);
    let mut add = fixture.add("part=a/bad-stats.parquet", "a", 9);
    add["add"]["stats"] = json!("not-json");
    fixture.commit(2, &[add]);
    let info = table::load(
        common::lake(),
        &load_request(fixture.path().to_string_lossy(), None),
    )
    .await
    .unwrap();
    let added = info
        .files
        .iter()
        .find(|file| file.path == "part=a/bad-stats.parquet")
        .unwrap();
    assert!(added.stats.is_none());
}

#[tokio::test]
async fn load_flattens_nested_null_counts() {
    let fixture = Fixture::new();
    write_parquet(&fixture.path().join("part=a/nested.parquet"), 9);
    let mut add = fixture.add("part=a/nested.parquet", "a", 9);
    add["add"]["stats"] = json!(json!({
        "numRecords": 9,
        "nullCount": {
            "id": 0,
            "nested": { "inner": { "x": 3, "y": 4 } }
        }
    })
    .to_string());
    fixture.commit(2, &[add]);
    let info = table::load(
        common::lake(),
        &load_request(fixture.path().to_string_lossy(), None),
    )
    .await
    .unwrap();
    let stats = info
        .files
        .iter()
        .find(|file| file.path == "part=a/nested.parquet")
        .unwrap()
        .stats
        .as_ref()
        .unwrap();
    assert_eq!(stats.null_count["id"], 0);
    assert_eq!(stats.null_count["nested.inner.x"], 3);
    assert_eq!(stats.null_count["nested.inner.y"], 4);
}

#[tokio::test]
async fn load_omits_stat_maps_when_disabled() {
    let fixture = Fixture::new();
    let info = table::load(
        common::lake(),
        &load_request(fixture.path().to_string_lossy(), None).with_file_stats(false),
    )
    .await
    .unwrap();
    let stats = info
        .files
        .iter()
        .find(|file| file.path == "part=a/added.parquet")
        .unwrap()
        .stats
        .as_ref()
        .unwrap();
    assert_eq!(stats.record_count, 9);
    assert!(stats.bytes_per_row.is_some());
    assert!(stats.first_values.is_empty());
    assert!(stats.last_values.is_empty());
    assert!(stats.null_count.is_empty());
    assert!(stats.tight_bounds.is_none());
}

#[tokio::test]
async fn load_reports_a_broken_snapshot() {
    let fixture = Fixture::new();
    fixture.commit(
        2,
        &[json!({"metaData": {
            "id": "967e1749-2635-481d-a114-897e027d7000",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": json!({"type": "struct", "fields": [
                {"name": "id", "type": "long", "nullable": false, "metadata": {}}
            ]}).to_string(),
            "partitionColumns": ["part"],
            "configuration": {},
            "createdTime": 0
        }})],
    );
    let error = table::load(
        common::lake(),
        &load_request(fixture.path().to_string_lossy(), None),
    )
    .await
    .err()
    .unwrap();
    let mut message = error.to_string();
    let mut source = std::error::Error::source(&error);
    while let Some(error) = source {
        message.push_str(": ");
        message.push_str(&error.to_string());
        source = error.source();
    }
    assert!(message.contains("Partition column"), "{message}");
}

#[tokio::test]
async fn checkpoint_preserves_active_files_without_json_history() {
    let fixture = Fixture::new();
    fixture.checkpoint().await;
    for version in 0..=1 {
        std::fs::remove_file(
            fixture
                .path()
                .join(format!("_delta_log/{version:020}.json")),
        )
        .unwrap();
    }
    let info = table::load(
        common::lake(),
        &load_request(fixture.path().to_string_lossy(), None),
    )
    .await
    .unwrap();
    let mut paths: Vec<_> = info.files.iter().map(|file| file.path.clone()).collect();
    paths.sort();
    assert_eq!(paths, ["part=a/added.parquet", "part=b/kept.parquet"]);
}
