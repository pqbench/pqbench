use pqbench::file::sample;
use pqbench::v1::{Blob, Bytemass, TableFile};

fn table_file(name: &str, size_bytes: u64) -> TableFile {
    TableFile {
        name: name.into(),
        size_bytes: Some(size_bytes),
        ..TableFile::default()
    }
}

#[test]
fn rate_one_keeps_every_file() {
    for index in 0..100 {
        let file = table_file(&format!("files/{index}"), 1_000_000);
        assert_eq!(sample::select(&file, 1.0).unwrap(), Some(1.0));
    }
}

#[test]
fn the_rate_is_the_probability_at_the_reference_size() {
    let rate = 0.1;
    let kept = (0..10_000)
        .filter(|index| {
            let file = table_file(&format!("files/{index}"), sample::REFERENCE_BYTES);
            sample::select(&file, rate).unwrap().is_some()
        })
        .count();
    assert!((900..=1100).contains(&kept), "kept {kept} of 10000");
}

#[test]
fn bigger_files_are_kept_more_often() {
    let kept = |size_bytes: u64| {
        (0..10_000)
            .filter(|index| {
                let file = table_file(&format!("files/{index}"), size_bytes);
                sample::select(&file, 0.1).unwrap().is_some()
            })
            .count()
    };
    let small = kept(sample::REFERENCE_BYTES / 4);
    let reference = kept(sample::REFERENCE_BYTES);
    let big = kept(sample::REFERENCE_BYTES * 4);
    assert!(small < reference, "{small} !< {reference}");
    assert!(reference < big, "{reference} !< {big}");
}

#[test]
fn a_file_large_enough_is_certain() {
    let file = table_file("files/large", sample::REFERENCE_BYTES * 10);
    assert_eq!(sample::select(&file, 0.1).unwrap(), Some(1.0));
    let file = table_file("files/larger", sample::REFERENCE_BYTES * 10_000);
    assert_eq!(sample::select(&file, 0.1).unwrap(), Some(1.0));
}

#[test]
fn a_higher_rate_never_drops_a_kept_file() {
    for index in 0..1_000 {
        let file = table_file(&format!("files/{index}"), sample::REFERENCE_BYTES / 2);
        let mut kept_before = false;
        for rate in [0.01, 0.02, 0.04, 0.08, 0.16, 0.32, 0.64, 1.0] {
            let kept = sample::select(&file, rate).unwrap().is_some();
            assert!(kept || !kept_before, "rate {rate} dropped a kept record");
            kept_before = kept;
        }
    }
}

#[test]
fn the_same_record_always_makes_the_same_decision() {
    let file = table_file("files/stable", sample::REFERENCE_BYTES);
    assert_eq!(
        sample::select(&file, 0.5).unwrap(),
        sample::select(&file, 0.5).unwrap()
    );
}

#[test]
fn every_kept_record_carries_its_probability() {
    let expected = 0.5 * (64.0 * 1024.0 * 1024.0) / sample::REFERENCE_BYTES as f64;
    let probabilities: Vec<f64> = (0..1_000)
        .filter_map(|index| {
            let file = table_file(&format!("files/{index}"), 64 * 1024 * 1024);
            sample::select(&file, 0.5).unwrap()
        })
        .collect();
    assert!(!probabilities.is_empty());
    for probability in probabilities {
        assert!((probability - expected).abs() < 1e-12);
    }
}

#[test]
fn a_file_without_a_positive_size_is_an_error() {
    let mut file = table_file("files/unknown", 1);
    file.size_bytes = None;
    assert!(sample::select(&file, 0.1).is_err());
    assert!(sample::select(&table_file("files/zero", 0), 0.1).is_err());
}

#[test]
fn a_rate_outside_the_range_is_an_error() {
    for rate in [0.0, -0.1, 1.1, f64::NAN] {
        assert!(sample::select(&table_file("files/x", 1), rate).is_err());
    }
}

#[test]
fn an_already_sampled_record_composes_with_the_smaller_rate() {
    let names: Vec<String> = (0..1_000).map(|index| format!("files/{index}")).collect();
    let kept = |previous: Option<f64>, rate: f64| -> Vec<(String, f64)> {
        names
            .iter()
            .filter_map(|name| {
                let mut file = table_file(name, sample::REFERENCE_BYTES);
                file.sample_rate = previous;
                sample::select(&file, rate)
                    .ok()
                    .flatten()
                    .map(|probability| (name.clone(), probability))
            })
            .collect()
    };
    let half = kept(None, 0.5);
    assert_eq!(kept(Some(0.5), 1.0), half);
    assert_eq!(kept(Some(1.0), 0.5), half);
    let tenth = kept(None, 0.1);
    assert_eq!(kept(Some(0.5), 0.1), tenth);
    assert!(tenth.len() < half.len());
}

#[test]
fn an_invalid_stored_rate_is_an_error() {
    for previous in [0.0, -0.1, 1.1, f64::NAN] {
        let mut file = table_file("files/x", sample::REFERENCE_BYTES);
        file.sample_rate = Some(previous);
        assert!(sample::select(&file, 1.0).is_err(), "{previous}");
    }
}

#[test]
fn a_rate_that_underflows_is_an_error() {
    assert!(sample::select(&table_file("files/x", 256), 1e-320).is_err());
}

#[test]
fn the_wire_omits_an_absent_rate() {
    let file = table_file("files/x", 1);
    assert!(!serde_json::to_string(&file)
        .unwrap()
        .contains("sample_rate"));
    let mut sampled = file;
    sampled.sample_rate = Some(0.25);
    let json = serde_json::to_string(&sampled).unwrap();
    assert!(json.contains("\"sample_rate\":0.25"), "{json}");
    let back: TableFile = serde_json::from_str(&json).unwrap();
    assert_eq!(back.sample_rate, Some(0.25));
    let back: TableFile = serde_json::from_str(r#"{"name":"files/x","size_bytes":1}"#).unwrap();
    assert_eq!(back.sample_rate, None);
}

#[test]
fn measurements_omit_an_absent_rate() {
    assert!(!serde_json::to_string(&Blob::default())
        .unwrap()
        .contains("sample_rate"));
    assert!(!serde_json::to_string(&Bytemass::default())
        .unwrap()
        .contains("sample_rate"));
    let blob = Blob {
        sample_rate: Some(0.5),
        ..Blob::default()
    };
    assert!(serde_json::to_string(&blob)
        .unwrap()
        .contains("\"sample_rate\":0.5"));
}
