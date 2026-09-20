//! The `bytemass` command: one typed request in, one typed output out.
//!
//! The CLI is a dump layer: it builds a [`BytemassRequest`], awaits [`bytemass`],
//! and prints the [`BytemassOutput`] through its `Display` rendering. Input
//! expansion, label building, measurement, aggregation, and the text / JSON /
//! d3 presentation all live behind that single function.

use std::fmt;

use serde::Serialize;

use crate::parquet_helpers::{Error, FileMass};

use super::analytics::{self, MassNode};
use super::collection::{self, MassSummary};
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
    /// On-disk file size in bytes.
    pub size: u64,
    /// Per-column masses read from the file's footer.
    pub mass: FileMass,
}

/// The command's typed result.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct BytemassOutput {
    /// Per-input measured masses, in input order.
    pub files: Vec<FileMassRecord>,
    /// Per-column totals across all inputs.
    pub summary: MassSummary,
    /// Byte-mass tree rooted at the input collection's label.
    pub tree: MassNode,
    /// Render the tree as composable JSON instead of text.
    pub is_json: bool,
    /// Render the tree as a self-contained d3 treemap page instead of text.
    pub is_d3: bool,
}

impl fmt::Display for BytemassOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_json {
            f.write_str(&json::tree(&self.tree).map_err(|_| fmt::Error)?)
        } else if self.is_d3 {
            f.write_str(&d3::render_html(&self.tree).map_err(|_| fmt::Error)?)
        } else {
            f.write_str(&text::render(&self.tree))
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
pub async fn bytemass(request: &BytemassRequest) -> Result<BytemassOutput, Error> {
    if request.inputs.is_empty() {
        return Err(Error("no inputs".into()));
    }
    if request.is_json == Some(true) && request.is_d3 == Some(true) {
        return Err(Error(
            "choose one output format: `is_json` or `is_d3`".into(),
        ));
    }
    let measured = collection::measure_inputs(&request.inputs).await?;
    let mut tree = analytics::aggregate(&raw::read(&measured.summary.file_mass()));
    tree.label = measured.label;
    Ok(BytemassOutput {
        files: measured.files,
        summary: measured.summary,
        tree,
        is_json: request.is_json == Some(true),
        is_d3: request.is_d3 == Some(true),
    })
}
