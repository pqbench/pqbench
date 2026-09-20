//! The `bytemass` command: one typed request in, one typed output out.
//!
//! The CLI is a dump layer: it builds a [`BytemassRequest`], calls [`bytemass`],
//! and prints the [`BytemassOutput`] through its `Display` rendering. Input
//! expansion, label building, measurement, aggregation, and the text / JSON /
//! d3 presentation all live behind that single function.

use std::collections::BTreeSet;
use std::fmt;
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::parquet_helpers::{default_metadata_parser, Error, FileMass, MetadataParser};

use super::analytics::{self, MassNode};
use super::collection::{MassAccumulator, MassSummary};
#[cfg(feature = "aws")]
use super::remote;
use super::{d3, json, raw, text};

/// Arguments for the `bytemass` command.
#[derive(Debug, Clone, Default)]
pub struct BytemassRequest {
    /// Parquet paths or glob masks; quote masks to prevent shell expansion.
    pub inputs: Vec<String>,
    /// Emit the byte-mass tree as composable JSON instead of text stats.
    pub is_json: Option<bool>,
    /// Emit a self-contained d3 treemap HTML instead of text stats.
    pub is_d3: Option<bool>,
}

/// One input's measured byte masses.
#[derive(Debug, Clone, Serialize)]
#[non_exhaustive]
pub struct FileMassRecord {
    /// Input path or URI as given.
    pub path: String,
    /// Per-column masses read from the file's footer.
    pub mass: FileMass,
}

/// The command's result: typed payloads plus the selected rendering.
#[derive(Debug, Clone, Serialize)]
#[non_exhaustive]
pub struct BytemassOutput {
    /// Per-input measured masses, in input order.
    pub files: Option<Vec<FileMassRecord>>,
    /// Per-column totals across all inputs.
    pub summary: Option<MassSummary>,
    /// Byte-mass tree rooted at the input collection's label.
    pub tree: Option<MassNode>,
    #[serde(skip)]
    rendering: Rendering,
}

/// How [`BytemassOutput`] renders through its `Display`.
#[derive(Debug, Clone, Copy, Default)]
enum Rendering {
    #[default]
    Text,
    Json,
    D3,
}

impl fmt::Display for BytemassOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Some(tree) = &self.tree else {
            return Ok(());
        };
        match self.rendering {
            Rendering::Text => f.write_str(&text::render(tree)),
            Rendering::Json => f.write_str(&json::tree(tree).map_err(|_| fmt::Error)?),
            Rendering::D3 => f.write_str(&d3::render_html(tree).map_err(|_| fmt::Error)?),
        }
    }
}

/// Measure the per-column byte masses of Parquet files.
///
/// Inputs are local paths or storage URIs, each possibly a glob mask for local
/// files. Only file footers are read. The output renders as text stats by
/// default, as composable JSON with [`BytemassRequest::is_json`], or as a
/// self-contained d3 treemap page with [`BytemassRequest::is_d3`].
///
/// # Errors
/// Fails when there are no inputs, a mask matches no files, both output formats
/// are requested, an input cannot be read, or a byte total overflows.
pub fn bytemass(request: &BytemassRequest) -> Result<BytemassOutput, Error> {
    if request.inputs.is_empty() {
        return Err(Error("no inputs".into()));
    }
    let rendering = rendering(request)?;
    let paths = expand_inputs(&request.inputs)?;
    let (files, summary) = measure(&paths)?;
    let mut tree = analytics::aggregate(&raw::read(&summary.file_mass()));
    tree.label = collection_label(&paths);
    Ok(BytemassOutput {
        files: Some(files),
        summary: Some(summary),
        tree: Some(tree),
        rendering,
    })
}

fn rendering(request: &BytemassRequest) -> Result<Rendering, Error> {
    match (request.is_json, request.is_d3) {
        (Some(true), Some(true)) => Err(Error(
            "choose one output format: `is_json` or `is_d3`".into(),
        )),
        (Some(true), _) => Ok(Rendering::Json),
        (_, Some(true)) => Ok(Rendering::D3),
        _ => Ok(Rendering::Text),
    }
}

fn measure(paths: &[PathBuf]) -> Result<(Vec<FileMassRecord>, MassSummary), Error> {
    let mut accumulator = MassAccumulator::new();
    let mut files = Vec::with_capacity(paths.len());
    for path in paths {
        let input = path.to_string_lossy().into_owned();
        let mass = read_input(&input)?;
        accumulator.add(mass.clone())?;
        files.push(FileMassRecord { path: input, mass });
    }
    Ok((files, accumulator.finish()))
}

#[cfg(feature = "aws")]
fn read_input(input: &str) -> Result<FileMass, Error> {
    if !input.contains("://") {
        return default_metadata_parser().read_masses(Path::new(input));
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| Error(format!("cannot start async runtime: {e}")))?;
    Ok(runtime.block_on(remote::read_remote(input))?.1)
}

#[cfg(not(feature = "aws"))]
fn read_input(input: &str) -> Result<FileMass, Error> {
    if input.contains("://") {
        return Err(Error(format!(
            "remote input {input} requires the `aws` feature"
        )));
    }
    default_metadata_parser().read_masses(Path::new(input))
}

fn expand_inputs(inputs: &[String]) -> Result<Vec<PathBuf>, Error> {
    let mut paths = BTreeSet::new();
    for input in inputs {
        if has_glob_metachar(input) {
            let mut matched = false;
            for entry in glob::glob(&escape_literal_brackets(input))
                .map_err(|e| Error(format!("invalid mask {input}: {e}")))?
            {
                let path = entry.map_err(|e| Error(format!("cannot expand mask {input}: {e}")))?;
                paths.insert(path);
                matched = true;
            }
            if !matched {
                return Err(Error(format!("mask matched no files: {input}")));
            }
        } else {
            paths.insert(PathBuf::from(input));
        }
    }
    Ok(paths.into_iter().collect())
}

fn has_glob_metachar(input: &str) -> bool {
    input.contains(['*', '?'])
}

fn escape_literal_brackets(input: &str) -> String {
    input.replace('[', "[[]")
}

fn collection_label(paths: &[PathBuf]) -> String {
    if let [path] = paths {
        return path
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_else(|| "file".into());
    }
    format!("{} parquet files", paths.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(name: &str) -> String {
        format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"))
    }

    fn request(inputs: Vec<String>) -> BytemassRequest {
        BytemassRequest {
            inputs,
            ..BytemassRequest::default()
        }
    }

    #[test]
    fn renders_text_stats_and_typed_payloads() {
        let output = bytemass(&request(vec![fixture("small_snappy.parquet")])).unwrap();
        let text = output.to_string();
        assert!(text.contains("bytemass: small_snappy.parquet"));
        assert!(text.contains("bytes/row"));
        assert!(text.contains("total"));

        let records = output.files.as_ref().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].path, fixture("small_snappy.parquet"));
        let summary = output.summary.as_ref().unwrap();
        assert_eq!(summary.file_count, 1);
        assert!(summary.num_rows > 0);
        assert!(output.tree.is_some());
    }

    #[test]
    fn renders_json_and_d3_when_requested() {
        let json = BytemassRequest {
            inputs: vec![fixture("small_snappy.parquet")],
            is_json: Some(true),
            ..BytemassRequest::default()
        };
        let tree = bytemass(&json).unwrap().to_string();
        assert!(tree.starts_with('{'));
        assert!(tree.contains("\"name\""));

        let d3 = BytemassRequest {
            inputs: vec![fixture("small_snappy.parquet")],
            is_d3: Some(true),
            ..BytemassRequest::default()
        };
        let page = bytemass(&d3).unwrap().to_string();
        assert!(page.starts_with("<!DOCTYPE html>"));
        assert!(page.contains("d3-hierarchy"));
    }

    #[test]
    fn expands_masks_and_labels_collections() {
        let mask = format!("{}/tests/fixtures/*.parquet", env!("CARGO_MANIFEST_DIR"));
        let output = bytemass(&request(vec![mask])).unwrap();
        assert!(output.summary.as_ref().unwrap().file_count > 1);
        assert!(output.to_string().contains("parquet files"));
    }

    #[test]
    fn rejects_empty_inputs_empty_masks_and_conflicting_formats() {
        assert!(bytemass(&request(vec![])).is_err());

        let missing = format!("{}/tests/fixtures/*.missing", env!("CARGO_MANIFEST_DIR"));
        let error = bytemass(&request(vec![missing])).unwrap_err().to_string();
        assert!(error.contains("mask matched no files"), "{error}");

        let conflict = BytemassRequest {
            inputs: vec![fixture("small_snappy.parquet")],
            is_json: Some(true),
            is_d3: Some(true),
        };
        assert!(bytemass(&conflict).is_err());
    }

    #[test]
    fn treats_brackets_as_literal_path_characters() {
        let dir = tempfile::tempdir().unwrap();
        let literal = dir.path().join("archive[1].parquet");
        std::fs::copy(fixture("small_snappy.parquet"), &literal).unwrap();
        let output = bytemass(&request(vec![literal.to_string_lossy().into_owned()])).unwrap();
        assert_eq!(output.files.as_ref().unwrap().len(), 1);
    }

    #[cfg(not(feature = "aws"))]
    #[test]
    fn names_the_missing_feature_for_remote_inputs() {
        let error = bytemass(&request(vec!["s3://bucket/file.parquet".into()]))
            .unwrap_err()
            .to_string();
        assert!(error.contains("aws"), "{error}");
    }
}
