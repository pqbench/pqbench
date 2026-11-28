//! `lint`: the storage a column's values do not need.
//!
//! `list suggestions` emits the rewrites a column plausibly allows; `get
//! suggestion` measures each against a control written by the same writer at
//! the same codec and level and marks it verified or not. A rule is one module
//! with a `detect` that reads only the source and a `measure` that runs the
//! controlled pair. The first rule that detects a column wins, so a column's
//! bytes are never counted twice.
//!
//! A suggestion carries the file's writer, because its remedy depends on what
//! that writer can change: parquet-mr, pyarrow, and parquet-rs take
//! per-column options, while Photon forwards them unverified.
//!
//! The sample is the file's one representative row group: the group whose row
//! count is closest to the file's average. A codec decision is made per column
//! chunk, so one average-sized chunk prices the file without an arbitrary byte
//! or row cap, and a short leading or trailing group never stands in for the
//! rest. Only the byte rules decode values; a codec suggestion rereads the
//! chunk's own pages, so a numeric column is never decoded to price it.
//!
//! Two rules read no file at all: `tombstones` and `small-files` decide from
//! the totals `list partitions` folded, so they hold for every partition,
//! including the ones a host with little network never copies.
//! [`partition_suggestions`] is that pair.

mod codec;
mod control;
mod delta_text;
mod dictionary;
mod int96;
mod partition;
mod price;
mod uuid;
mod writer;

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::path::Path;

use parquet::basic::Encoding;
use parquet::column::reader::{ColumnReader, ColumnReaderImpl};
use parquet::data_type::{ByteArray, DataType};
use parquet::file::metadata::{
    ColumnChunkMetaData, PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader,
};
use parquet::file::reader::{FileReader, RowGroupReader, SerializedFileReader};

use crate::third_party::parquet::api::Error as ParquetError;
use crate::v1;

use self::control::Measurement;
use self::writer::Writer;

pub use codec::CODEC_ZSTD_RULE;
pub use delta_text::DELTA_TEXT_ENCODING_RULE;
pub use dictionary::{DICTIONARY_MISSING_RULE, DICTIONARY_UNIQUE_RULE};
pub use int96::INT96_TIMESTAMP_RULE;
pub use partition::{partition_suggestions, SMALL_FILES_RULE, TOMBSTONES_RULE};
pub use uuid::UUID_TEXT_RULE;

/// Wins smaller than this are noted as noise, not dropped.
const NOISE_FLOOR_BYTES: u64 = 64 * 1024;

/// A file at least this large is a real table's file rather than a fixture: it
/// can hold hundreds of columns, so a column that cannot clear the noise floor
/// is skipped before it is decoded.
const LARGE_FILE_BYTES: u64 = 1024 * 1024;

/// Errors from the lint layer.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The parquet layer failed.
    Parquet(ParquetError),
    /// A suggestion named a rule this build does not have.
    Rule(String),
    /// The input record was invalid or did not match the file.
    Invalid(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Parquet(_) => write!(f, "the parquet layer failed"),
            Self::Rule(rule) => write!(f, "unknown suggestion rule `{rule}`"),
            Self::Invalid(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Parquet(error) => Some(error),
            Self::Rule(_) | Self::Invalid(_) => None,
        }
    }
}

impl From<ParquetError> for Error {
    fn from(error: ParquetError) -> Self {
        Self::Parquet(error)
    }
}

impl From<parquet::errors::ParquetError> for Error {
    fn from(error: parquet::errors::ParquetError) -> Self {
        Self::Parquet(ParquetError::Parquet(Box::new(error)))
    }
}

impl From<crate::bytemass::Error> for Error {
    fn from(error: crate::bytemass::Error) -> Self {
        Self::Invalid(error.to_string())
    }
}

impl From<crate::codecs::Error> for Error {
    fn from(error: crate::codecs::Error) -> Self {
        Self::Invalid(error.to_string())
    }
}

/// What a sample decoded: rows and the whole row groups they came from.
#[derive(Clone, Copy, Default)]
struct Coverage {
    row_count: u64,
    row_group_count: u64,
}

/// What a column chunk's footer records about its dictionary page.
#[derive(Clone, Copy, Default)]
struct ChunkDictionary {
    /// The chunk has a dictionary page.
    present: bool,
    /// Bytes from the dictionary page offset to the first data page.
    page_size: Option<usize>,
    /// The chunk's data pages include PLAIN beside a dictionary encoding, so
    /// its writer fell back to PLAIN.
    fell_back: bool,
}

/// One requested column's representative row group chunk.
#[derive(Default)]
struct DecodedColumn {
    /// One cell per row of the group; `None` where the column is null. Empty
    /// for a non-byte leaf, whose rules read the footer or the raw pages.
    cells: Vec<Option<ByteArray>>,
    /// Non-null values in the chunk, exact when the cells were decoded and
    /// from the footer statistics otherwise.
    non_null_value_count: u64,
    /// The chunk's compressed bytes.
    compressed_bytes: u64,
    /// The chunk's dictionary page, as its footer records it.
    dictionary: ChunkDictionary,
    /// The chunk's leaf index in the file schema.
    column_index: usize,
}

/// The representative row group's whole chunks for the requested scalar leaves.
struct DecodedLeaves {
    /// Per requested column: its chunk from the representative row group.
    columns: Vec<DecodedColumn>,
    coverage: Coverage,
    /// The representative row group's index, or zero when the file has none.
    row_group: usize,
}

/// One measured column and its decoded sample: what a rule detects and
/// measures.
pub(crate) struct ColumnSample<'a> {
    blob: &'a str,
    leaf: &'a str,
    column: &'a v1::Column,
    mass: &'a v1::Bytemass,
    /// The file the sample was decoded from, so a rule can read raw pages.
    path: &'a Path,
    /// The sample's row group index in that file.
    row_group: usize,
    /// The sample's leaf index in that file's schema.
    column_index: usize,
    /// The representative row group's whole chunk, one cell per row; empty for
    /// a non-byte leaf.
    cells: &'a [Option<ByteArray>],
    /// The sample chunk's real on-disk compressed bytes.
    chunk_compressed_bytes: u64,
    /// The sample chunk wrote a dictionary page.
    chunk_has_dictionary: bool,
    /// Bytes from the sample chunk's dictionary page offset to its first data
    /// page.
    chunk_dictionary_page_size: Option<usize>,
    /// The sample chunk's data pages include PLAIN beside a dictionary
    /// encoding.
    chunk_fell_back: bool,
    writer: Option<&'a str>,
    coverage: Coverage,
    text_bpv: f64,
}

impl<'a> ColumnSample<'a> {
    /// The sample a rule reads, or `None` when the footer lacks the counts or
    /// the sample has no non-null value.
    fn new(
        path: &'a Path,
        blob: &'a str,
        leaf: &'a str,
        column: &'a v1::Column,
        decoded: &'a DecodedLeaves,
        at: usize,
        writer: Option<&'a str>,
    ) -> Option<Self> {
        let mass = column.mass.as_ref()?;
        let chunk = decoded.columns.get(at)?;
        let non_null = chunk.non_null_value_count;
        if non_null == 0 {
            return None;
        }
        let chunk_compressed_bytes = chunk.compressed_bytes;
        let text_bpv = if chunk_compressed_bytes > 0 {
            chunk_compressed_bytes as f64 / non_null as f64
        } else {
            let compressed_bytes = mass.compressed_bytes?;
            let value_count = mass.value_count?;
            let non_null_values = match mass.null_count {
                Some(nulls) => value_count.saturating_sub(nulls),
                None if chunk.cells.is_empty() => value_count,
                None => {
                    let fraction = non_null as f64 / chunk.cells.len().max(1) as f64;
                    (value_count as f64 * fraction).round() as u64
                }
            };
            if non_null_values == 0 {
                return None;
            }
            compressed_bytes as f64 / non_null_values as f64
        };
        Some(Self {
            blob,
            leaf,
            column,
            mass,
            path,
            row_group: decoded.row_group,
            column_index: chunk.column_index,
            cells: chunk.cells.as_slice(),
            chunk_compressed_bytes,
            chunk_has_dictionary: chunk.dictionary.present,
            chunk_dictionary_page_size: chunk.dictionary.page_size,
            chunk_fell_back: chunk.dictionary.fell_back,
            writer,
            coverage: decoded.coverage,
            text_bpv,
        })
    }

    /// Whether the leaf is a `BYTE_ARRAY`, the physical type the text rules
    /// read.
    fn byte_array(&self) -> bool {
        self.column.physical_data_type.as_deref() == Some("BYTE_ARRAY")
    }

    /// Whether the leaf is a byte leaf (`BYTE_ARRAY` or
    /// `FIXED_LEN_BYTE_ARRAY`), the physical types the byte rules decode.
    fn byte_leaf(&self) -> bool {
        matches!(
            self.column.physical_data_type.as_deref(),
            Some("BYTE_ARRAY" | "FIXED_LEN_BYTE_ARRAY")
        )
    }

    /// The dictionary page size limit that reproduces the sample chunk's own
    /// behavior. A chunk that wrote a dictionary page and never fell back must
    /// not fall back in the control either, or the arms would compare a
    /// dictionary against PLAIN and credit a rewrite with the fallback's loss; a
    /// chunk that did fall back hit its writer's limit, which the dictionary
    /// page it had written by then approximates. `None` keeps parquet-rs's
    /// 1 MiB default.
    fn dictionary_page_size_limit(&self) -> Option<usize> {
        match (self.chunk_has_dictionary, self.chunk_fell_back) {
            (true, false) => Some(usize::MAX),
            (true, true) => self.chunk_dictionary_page_size.filter(|bytes| *bytes > 0),
            (false, _) => None,
        }
    }

    /// The writer this file came from, as the rules' remedies need it.
    fn writer_kind(&self) -> Writer {
        Writer::classify(self.writer)
    }

    /// A sample over one chunk, for the rules' unit tests.
    #[cfg(test)]
    pub(crate) fn for_test(
        column: &'a v1::Column,
        cells: &'a [Option<ByteArray>],
        chunk_has_dictionary: bool,
    ) -> Self {
        let mass = column.mass.as_ref().expect("a sample column has a mass");
        let non_null = cells.iter().filter(|cell| cell.is_some()).count().max(1) as u64;
        let chunk_compressed_bytes = mass.compressed_bytes.unwrap_or(0);
        Self {
            blob: "blobs/s3/example-bucket/data.parquet",
            leaf: "id",
            column,
            mass,
            path: Path::new(""),
            row_group: 0,
            column_index: 0,
            cells,
            chunk_compressed_bytes,
            chunk_has_dictionary,
            chunk_dictionary_page_size: None,
            chunk_fell_back: false,
            writer: Some("parquet-rs version 59.3.2"),
            coverage: Coverage {
                row_count: cells.len() as u64,
                row_group_count: 1,
            },
            text_bpv: chunk_compressed_bytes as f64 / non_null as f64,
        }
    }

    /// One potential suggestion, on the column's own name and context.
    fn candidate(&self, rule: &'static str, finding: Finding) -> v1::Suggestion {
        v1::Suggestion {
            name: v1::name::suggestion(self.blob, self.leaf, rule),
            rule: rule.to_string(),
            column: self.leaf.to_string(),
            table: self.column.table.clone(),
            partition: None,
            sample_rate: self.column.sample_rate,
            size_bytes: self.column.size_bytes,
            writer: self.writer.map(ToString::to_string),
            message: finding.message,
            remedy: finding.remedy,
            remedies: finding.remedies,
            verified: None,
            savings_bytes: None,
            sample_row_count: Some(self.coverage.row_count),
            sample_row_group_count: Some(self.coverage.row_group_count),
        }
    }
}

/// What a rule found and how to measure its rewrite.
pub(crate) struct Finding {
    pub(crate) message: String,
    pub(crate) remedy: String,
    /// The same advice as one setting per engine, so a reader can apply it in
    /// whichever engine compacts the table.
    pub(crate) remedies: Vec<v1::Remedy>,
    pub(crate) measure: Measure,
}

/// A rule's setting for `engine`, from its remedy table: empty when the engine
/// has none.
pub(crate) fn setting(remedies: &[v1::Remedy], engine: &str) -> String {
    remedies
        .iter()
        .find(|remedy| remedy.engine == engine)
        .map_or_else(String::new, |remedy| remedy.setting.clone())
}

/// A rule's controlled measurement of its rewrite.
pub(crate) type Measure = fn(&ColumnSample<'_>) -> Result<Option<Measurement>, Error>;

/// A rule: the detection that makes a column a candidate, and the control that
/// verifies it.
pub(crate) struct Rule {
    pub(crate) id: &'static str,
    pub(crate) detect: fn(&ColumnSample<'_>) -> Option<Finding>,
}

/// The rules, in priority order. The first that detects a column wins, so a
/// column's bytes are priced once and two rules never count the same saving.
///
/// `codec-zstd` outranks the encoding rules because its win is the larger one:
/// a snappy or uncompressed file's 35-60% codec upgrade must not be masked by a
/// 0-1% dictionary or Delta encoding tweak. A zstd file never reaches it, so
/// its columns fall through to the encoding rules.
pub(crate) const RULES: &[Rule] = &[
    Rule {
        id: UUID_TEXT_RULE,
        detect: uuid::detect,
    },
    Rule {
        id: INT96_TIMESTAMP_RULE,
        detect: int96::detect,
    },
    Rule {
        id: CODEC_ZSTD_RULE,
        detect: codec::detect,
    },
    Rule {
        id: DICTIONARY_UNIQUE_RULE,
        detect: dictionary::detect_unique,
    },
    Rule {
        id: DICTIONARY_MISSING_RULE,
        detect: dictionary::detect_missing,
    },
    Rule {
        id: DELTA_TEXT_ENCODING_RULE,
        detect: delta_text::detect,
    },
];

/// The rule with this id, when this build has it.
fn rule(id: &str) -> Option<&'static Rule> {
    RULES.iter().find(|rule| rule.id == id)
}

/// Lint one local Parquet file into potential suggestions.
///
/// `file` carries the table file's identity and blob name, so each suggestion
/// names the blob the walk read, not the local path a caller may have copied it
/// to. No rewrite is encoded here: `verify` does that. The sample is the file's
/// one representative row group, because a codec decision is made per column
/// chunk. A 1 MiB+ file skips the columns whose chunk cannot clear the noise
/// floor without decoding them; a smaller file is a fixture whose every column
/// is measured.
///
/// # Errors
/// Fails when `file` has no blob name or the file cannot be read.
pub fn suggestions(path: &Path, file: &v1::TableFile) -> Result<Vec<v1::Suggestion>, Error> {
    let blob_name = file
        .blob
        .as_deref()
        .ok_or_else(|| Error::Invalid("a table-file record needs a blob to read".into()))?;
    let mut measured = crate::bytemass::measure_footer(blob_name, path)?;
    let table = v1::TableFile::parse_name(&file.name)
        .map(|(catalog, schema, table, _)| v1::name::table(&catalog, &schema, &table));
    for column in &mut measured.columns {
        column.table = table.clone();
        column.sample_rate = file.sample_rate;
        column.size_bytes = measured.blob.size_bytes;
    }
    let blob = &measured.blob;
    let columns = &measured.columns;
    let large_file = blob
        .size_bytes
        .is_some_and(|bytes| bytes >= LARGE_FILE_BYTES);
    let targets: Vec<(&v1::Column, String)> = columns
        .iter()
        .filter(|column| column.max_repetition_level == Some(0))
        .filter(|column| !large_file || clears_noise_floor(column))
        .filter_map(|column| {
            let (_, _, leaf) = v1::Column::parse_name(&column.name)?;
            Some((column, leaf))
        })
        .collect();
    if targets.is_empty() {
        return Ok(Vec::new());
    }
    let leaves: Vec<String> = targets.iter().map(|(_, leaf)| leaf.clone()).collect();
    let decoded = decode_leaves(path, &leaves)?;
    let mut suggestions = Vec::new();

    for (at, (column, leaf)) in targets.iter().enumerate() {
        let Some(sample) = ColumnSample::new(
            path,
            &blob.name,
            leaf,
            column,
            &decoded,
            at,
            blob.writer.as_deref(),
        ) else {
            continue;
        };
        let compressed = column
            .mass
            .as_ref()
            .and_then(|mass| mass.compressed_bytes)
            .unwrap_or(0);
        for rule in RULES {
            if let Some(finding) = (rule.detect)(&sample) {
                suggestions.push((compressed, sample.candidate(rule.id, finding)));
                break;
            }
        }
    }

    suggestions.sort_by(|left, right| {
        right
            .0
            .cmp(&left.0)
            .then_with(|| left.1.column.cmp(&right.1.column))
    });
    Ok(suggestions
        .into_iter()
        .map(|(_, suggestion)| suggestion)
        .collect())
}

/// Whether a column's chunk holds at least the noise floor. A column below it
/// cannot earn a win that could be told from noise, so a wide file's null or
/// short leaves never reach a rule.
fn clears_noise_floor(column: &v1::Column) -> bool {
    column
        .mass
        .as_ref()
        .and_then(|mass| mass.compressed_bytes)
        .is_none_or(|bytes| bytes >= NOISE_FLOOR_BYTES)
}

/// Measure one potential suggestion against its control.
///
/// The sample is the same representative row group `list` decoded, so
/// `list | get` measures the same values. The record then carries the rows
/// actually read.
///
/// # Errors
/// Fails when the suggestion is invalid, the sample cannot be read, or the rule
/// is unknown.
pub fn verify(path: &Path, candidate: &v1::Suggestion) -> Result<v1::Suggestion, Error> {
    let (service, locator, column_name, rule_id) = v1::Suggestion::parse_name(&candidate.name)
        .ok_or_else(|| Error::Invalid(format!("not a suggestion name: {}", candidate.name)))?;
    if candidate.column != column_name || candidate.rule != rule_id {
        return Err(Error::Invalid(format!(
            "the suggestion name names `{column_name}` / `{rule_id}`, its fields say `{}` / `{}`",
            candidate.column, candidate.rule
        )));
    }
    let blob_name = v1::name::blob(&service, &locator);
    let measured = crate::bytemass::measure_footer(&blob_name, path)?;
    let blob = &measured.blob;
    let column = measured
        .columns
        .iter()
        .find(|measured| {
            v1::Column::parse_name(&measured.name)
                .is_some_and(|(_, _, leaf)| leaf == candidate.column)
        })
        .ok_or_else(|| Error::Invalid(format!("the file has no column `{}`", candidate.column)))?;
    if column.max_repetition_level != Some(0) {
        return Ok(reject(
            candidate,
            blob,
            None,
            "the column is repeated; suggestions measure scalar leaves only",
        ));
    }
    let leaf = candidate.column.clone();
    let decoded = decode_leaves(path, std::slice::from_ref(&leaf))?;
    let coverage = decoded.coverage;
    let Some(sample) = ColumnSample::new(
        path,
        &blob.name,
        &leaf,
        column,
        &decoded,
        0,
        blob.writer.as_deref(),
    ) else {
        return Ok(reject(
            candidate,
            blob,
            Some(coverage),
            "the sample has no non-null value",
        ));
    };
    let Some(rule) = rule(&candidate.rule) else {
        return Err(Error::Rule(candidate.rule.clone()));
    };
    let Some(finding) = (rule.detect)(&sample) else {
        return Ok(reject(
            candidate,
            blob,
            Some(coverage),
            "the rule no longer detects this column",
        ));
    };
    let measurement = (finding.measure)(&sample)?;
    let compressed = column
        .mass
        .as_ref()
        .and_then(|mass| mass.compressed_bytes)
        .unwrap_or(0);
    Ok(enriched(
        candidate,
        blob,
        coverage,
        finding,
        measurement,
        compressed,
    ))
}

/// A candidate the control could not confirm as it stands.
fn reject(
    candidate: &v1::Suggestion,
    blob: &v1::Blob,
    coverage: Option<Coverage>,
    reason: &str,
) -> v1::Suggestion {
    let mut rejected = candidate.clone();
    rejected.writer = blob.writer.clone();
    rejected.size_bytes = blob.size_bytes;
    if let Some(coverage) = coverage {
        rejected.sample_row_count = Some(coverage.row_count);
        rejected.sample_row_group_count = Some(coverage.row_group_count);
    }
    rejected.verified = Some(false);
    rejected.savings_bytes = None;
    rejected.message = format!("{}; {reason}", candidate.message);
    rejected
}

/// A candidate the control measured.
fn enriched(
    candidate: &v1::Suggestion,
    blob: &v1::Blob,
    coverage: Coverage,
    finding: Finding,
    measurement: Option<Measurement>,
    compressed_bytes: u64,
) -> v1::Suggestion {
    let mut enriched = candidate.clone();
    enriched.writer = blob.writer.clone();
    enriched.size_bytes = blob.size_bytes;
    enriched.sample_row_count = Some(coverage.row_count);
    enriched.sample_row_group_count = Some(coverage.row_group_count);
    enriched.remedy = finding.remedy;
    enriched.remedies = finding.remedies;
    let Some(measurement) = measurement else {
        enriched.verified = None;
        enriched.savings_bytes = None;
        enriched.message = format!(
            "{}; not measurable at this sample: the writer cannot reproduce the file's codec",
            finding.message
        );
        return enriched;
    };
    let saving = (compressed_bytes as f64 * measurement.ratio_low)
        .round()
        .max(0.0) as u64;
    let verified = measurement.ratio_low > 0.0 && saving > 0;
    enriched.verified = Some(verified);
    enriched.savings_bytes = verified.then_some(saving);
    enriched.message = format!(
        "{}; {}",
        finding.message,
        verdict(&measurement, saving, verified)
    );
    enriched
}

/// The control's result in words.
fn verdict(measurement: &Measurement, saving: u64, verified: bool) -> String {
    let low = measurement.ratio_low * 100.0;
    let high = measurement.ratio_high * 100.0;
    if verified {
        let smaller = if (high - low).abs() < 0.1 {
            format!(
                "the controlled rewrite is {}% smaller at {}",
                percent(low),
                measurement.levels
            )
        } else {
            format!(
                "the controlled rewrite is {}-{}% smaller at {}",
                percent(low),
                percent(high),
                measurement.levels
            )
        };
        if saving < NOISE_FLOOR_BYTES {
            format!("{smaller}; {saving} bytes is below the 64 KiB noise floor")
        } else {
            smaller
        }
    } else if measurement.ratio_high <= 0.0 {
        format!(
            "the controlled rewrite is not smaller at {} (best {high:+.1}%)",
            measurement.levels
        )
    } else if measurement.ratio_low <= 0.0 {
        format!(
            "the controlled rewrite wins only at some {} settings (best {}%)",
            measurement.levels,
            percent(high)
        )
    } else {
        format!("the controlled rewrite saves only {saving} bytes, below the 64 KiB noise floor")
    }
}

/// A percentage with enough precision to show a small win.
fn percent(value: f64) -> String {
    if value < 0.1 {
        "<0.1".to_string()
    } else if value < 10.0 {
        format!("{value:.1}")
    } else {
        format!("{value:.0}")
    }
}

/// Decode the file's one representative row group for the requested scalar
/// leaf columns, directly from their column chunks. Values are decoded only for
/// a byte leaf; any other leaf carries its footer counts, and a page-reading
/// rule rereads its raw pages. Unselected columns are never read.
fn decode_leaves(path: &Path, leaves: &[String]) -> Result<DecodedLeaves, Error> {
    let file = std::fs::File::open(path).map_err(ParquetError::Io)?;
    let reader = SerializedFileReader::new(file)?;
    let metadata = reader.metadata();
    let schema = metadata.file_metadata().schema_descr();
    let leaf_index: HashMap<String, usize> = schema
        .columns()
        .iter()
        .enumerate()
        .filter(|(_, descr)| descr.max_rep_level() == 0)
        .map(|(at, descr)| (descr.path().string(), at))
        .collect();
    let Some(row_group) = representative_row_group(metadata) else {
        return Ok(DecodedLeaves {
            columns: leaves.iter().map(|_| DecodedColumn::default()).collect(),
            coverage: Coverage::default(),
            row_group: 0,
        });
    };
    let group = reader.get_row_group(row_group)?;
    let row_count = usize::try_from(group.metadata().num_rows()).unwrap_or(0);
    let mut columns = Vec::with_capacity(leaves.len());
    for leaf in leaves {
        let Some(col_idx) = leaf_index.get(leaf).copied() else {
            columns.push(DecodedColumn::default());
            continue;
        };
        let meta = group.metadata().column(col_idx);
        let descr = schema.column(col_idx);
        // Only the byte rules decode cells; every other rule reads the footer
        // or the raw pages, so a numeric column is never decoded here.
        let cells = if byte_physical_type(descr.physical_type()) {
            decode_bytes_chunk(group.as_ref(), col_idx, descr.max_def_level(), row_count)?
        } else {
            Vec::new()
        };
        let non_null_value_count = if cells.is_empty() {
            chunk_non_null(meta)
        } else {
            cells.iter().filter(|cell| cell.is_some()).count() as u64
        };
        columns.push(DecodedColumn {
            cells,
            non_null_value_count,
            compressed_bytes: u64::try_from(meta.compressed_size()).unwrap_or(0),
            dictionary: chunk_dictionary(meta),
            column_index: col_idx,
        });
    }
    Ok(DecodedLeaves {
        columns,
        coverage: Coverage {
            row_count: row_count as u64,
            row_group_count: 1,
        },
        row_group,
    })
}

/// Whether a physical type is a byte leaf: the types the byte rules decode.
fn byte_physical_type(physical: parquet::basic::Type) -> bool {
    matches!(
        physical,
        parquet::basic::Type::BYTE_ARRAY | parquet::basic::Type::FIXED_LEN_BYTE_ARRAY
    )
}

/// The non-null values a chunk's footer records: its value count less the null
/// statistics, when the writer recorded them.
fn chunk_non_null(meta: &ColumnChunkMetaData) -> u64 {
    let values = u64::try_from(meta.num_values()).unwrap_or(0);
    let nulls = meta
        .statistics()
        .and_then(|statistics| statistics.null_count_opt())
        .unwrap_or(0);
    values.saturating_sub(nulls)
}

/// The row group whose row count is closest to the file's average, breaking
/// ties by the lowest index.
///
/// One average-sized group prices a codec decision on whole values, while a
/// short leading group or a short trailing tail group would not stand for the
/// rest of the file.
fn representative_row_group(metadata: &ParquetMetaData) -> Option<usize> {
    let group_count = metadata.num_row_groups();
    if group_count == 0 {
        return None;
    }
    let total_rows: i64 = metadata
        .row_groups()
        .iter()
        .map(|group| group.num_rows())
        .sum();
    let average_rows = total_rows as f64 / group_count as f64;
    let mut representative = 0;
    let mut closest = f64::INFINITY;
    for (at, group) in metadata.row_groups().iter().enumerate() {
        let distance = (group.num_rows() as f64 - average_rows).abs();
        if distance < closest {
            representative = at;
            closest = distance;
        }
    }
    Some(representative)
}

/// What one column chunk's footer records about its dictionary page.
fn chunk_dictionary(meta: &ColumnChunkMetaData) -> ChunkDictionary {
    let dictionary_page_offset = meta.dictionary_page_offset();
    let data_page_offset = meta.data_page_offset();
    // A dictionary page's values are PLAIN-encoded, so a chunk's whole encoding
    // set always lists PLAIN beside its dictionary encoding; only the data
    // pages' encodings tell a falling-back chunk from a dictionary-encoded one.
    // A footer without page encoding stats cannot say, so it stays a dictionary
    // chunk: the control then prices a dictionary the source might not have
    // used, understating the win rather than overstating it.
    let data_encodings: Vec<Encoding> = meta
        .page_encoding_stats_mask()
        .map(|mask| mask.encodings().collect())
        .unwrap_or_default();
    let plain = data_encodings.contains(&Encoding::PLAIN);
    let dictionary_encoding = data_encodings.iter().any(|encoding| {
        matches!(
            encoding,
            Encoding::RLE_DICTIONARY | Encoding::PLAIN_DICTIONARY
        )
    });
    ChunkDictionary {
        present: dictionary_page_offset.is_some(),
        page_size: dictionary_page_offset
            .filter(|offset| data_page_offset > *offset)
            .and_then(|offset| usize::try_from(data_page_offset - offset).ok()),
        fell_back: plain && dictionary_encoding,
    }
}

fn decode_bytes_chunk(
    group: &dyn RowGroupReader,
    col_idx: usize,
    max_def: i16,
    rows: usize,
) -> Result<Vec<Option<ByteArray>>, Error> {
    match group.get_column_reader(col_idx)? {
        ColumnReader::ByteArrayColumnReader(mut reader) => {
            decode_typed(&mut reader, max_def, rows, |value| value.clone())
        }
        ColumnReader::FixedLenByteArrayColumnReader(mut reader) => {
            decode_typed(&mut reader, max_def, rows, |value| {
                ByteArray::from(value.data())
            })
        }
        _ => Err(Error::Invalid(format!(
            "column {col_idx} is not a BYTE_ARRAY or FIXED_LEN_BYTE_ARRAY leaf; \
             the byte rules decode only those"
        ))),
    }
}

/// One page of a column chunk, read from the file's bytes: the payload a codec
/// compresses, and, when the file records an offset index, the bytes the page
/// occupies on disk.
pub(crate) struct ChunkPage {
    /// The page's uncompressed payload: exactly the bytes a codec compresses.
    pub(crate) payload: Vec<u8>,
    /// The page's on-disk bytes, page header included; `None` when the file
    /// records no offset index.
    pub(crate) compressed_bytes: Option<u64>,
    /// The page holds values: a data page, not the chunk's dictionary page.
    pub(crate) data: bool,
}

/// The sample chunk's pages, read straight from the file: the bytes a codec
/// compresses, without decoding values.
pub(crate) fn chunk_pages(sample: &ColumnSample<'_>) -> Result<Vec<ChunkPage>, Error> {
    let file = std::fs::File::open(sample.path).map_err(ParquetError::Io)?;
    let reader = SerializedFileReader::new(file)?;
    let recorded_bytes = chunk_page_compressed_bytes(sample, reader.metadata())?;
    let group = reader.get_row_group(sample.row_group)?;
    let mut pages = group.get_column_page_reader(sample.column_index)?;
    let mut read_pages = Vec::new();
    let mut data_at = 0;
    for page in &mut pages {
        let page = page?;
        let data = page.is_data_page();
        // The offset index lists the chunk's data pages only, in file order.
        let compressed_bytes = if data {
            let bytes = recorded_bytes.get(data_at).copied().flatten();
            data_at += 1;
            bytes
        } else {
            None
        };
        read_pages.push(ChunkPage {
            payload: page.buffer().to_vec(),
            compressed_bytes,
            data,
        });
    }
    Ok(read_pages)
}

/// The sample chunk's data pages' on-disk bytes, page header included, as the
/// file's offset index records them: one entry per data page, in file order.
///
/// A `Page` hands back a decompressed buffer and no size of its own, and a
/// `PageMetadata` carries no size either, so the offset index is the public
/// source for a single page's on-disk bytes. A file that records no offset
/// index returns an empty list: its chunk's footer bytes are then the only
/// on-disk number known.
fn chunk_page_compressed_bytes(
    sample: &ColumnSample<'_>,
    metadata: &ParquetMetaData,
) -> Result<Vec<Option<u64>>, Error> {
    let Some(chunk) = metadata
        .row_groups()
        .get(sample.row_group)
        .and_then(|group| group.columns().get(sample.column_index))
    else {
        return Ok(Vec::new());
    };
    if chunk.offset_index_offset().is_none() {
        return Ok(Vec::new());
    }
    let file = std::fs::File::open(sample.path).map_err(ParquetError::Io)?;
    // A footer whose offset index cannot be read costs the page granularity,
    // not the measurement: whole-chunk pricing stays exact.
    let Ok(indexed) = ParquetMetaDataReader::new()
        .with_offset_index_policy(PageIndexPolicy::Optional)
        .parse_and_finish(&file)
    else {
        return Ok(Vec::new());
    };
    let Some(index) = indexed
        .offset_index()
        .and_then(|row_groups| row_groups.get(sample.row_group))
        .and_then(|columns| columns.get(sample.column_index))
    else {
        return Ok(Vec::new());
    };
    Ok(index
        .page_locations()
        .iter()
        .map(|location| u64::try_from(location.compressed_page_size).ok())
        .collect())
}

/// The fraction of sampled non-null values that are distinct within the chunk
/// (Parquet builds one dictionary per row-group column chunk).
pub(crate) fn unique_fraction(cells: &[Option<ByteArray>]) -> f64 {
    let mut distinct: HashSet<&[u8]> = HashSet::new();
    let mut non_null_total = 0usize;
    for bytes in cells.iter().flatten() {
        non_null_total += 1;
        distinct.insert(bytes.data());
    }
    distinct.len() as f64 / non_null_total.max(1) as f64
}

fn decode_typed<T: DataType>(
    reader: &mut ColumnReaderImpl<T>,
    max_def: i16,
    rows: usize,
    mut to_bytes: impl FnMut(&T::T) -> ByteArray,
) -> Result<Vec<Option<ByteArray>>, Error> {
    let mut out = Vec::with_capacity(rows);
    let mut remaining = rows;
    let mut values = Vec::new();
    let mut def_levels = Vec::new();
    while remaining > 0 {
        let batch = remaining.min(1024);
        values.clear();
        def_levels.clear();
        let (_, _, levels) = reader.read_records(
            batch,
            (max_def > 0).then_some(&mut def_levels),
            None,
            &mut values,
        )?;
        let consumed = if max_def > 0 { levels } else { values.len() };
        if consumed == 0 {
            break;
        }
        if max_def == 0 {
            out.extend(values.iter().map(|v| Some(to_bytes(v))));
        } else {
            let mut at = 0;
            for &def in &def_levels[..levels] {
                if def == max_def {
                    out.push(Some(to_bytes(&values[at])));
                    at += 1;
                } else {
                    out.push(None);
                }
            }
        }
        remaining = remaining.saturating_sub(consumed);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use parquet::data_type::ByteArray;
    use parquet::file::metadata::{FileMetaData, ParquetMetaData, RowGroupMetaData};
    use parquet::schema::types::{SchemaDescriptor, Type};

    use super::representative_row_group;
    use super::{
        rule, ColumnSample, CODEC_ZSTD_RULE, DELTA_TEXT_ENCODING_RULE, DICTIONARY_MISSING_RULE,
        DICTIONARY_UNIQUE_RULE, INT96_TIMESTAMP_RULE, UUID_TEXT_RULE,
    };
    use crate::v1;

    /// A `BYTE_ARRAY` column whose chunk is stored at `codec`.
    fn byte_column(codec: &str) -> v1::Column {
        v1::Column {
            physical_data_type: Some("BYTE_ARRAY".into()),
            mass: Some(v1::Bytemass {
                codec: Some(codec.into()),
                ..v1::Bytemass::default()
            }),
            ..v1::Column::default()
        }
    }

    /// A column of `physical_data_type` stored at `codec`.
    fn typed_column(physical_data_type: &str, codec: &str) -> v1::Column {
        v1::Column {
            physical_data_type: Some(physical_data_type.into()),
            mass: Some(v1::Bytemass {
                codec: Some(codec.into()),
                ..v1::Bytemass::default()
            }),
            ..v1::Column::default()
        }
    }

    /// Canonical UUID text for `value`.
    fn uuid_text(value: usize) -> String {
        format!("{value:08x}-0000-4000-8000-{value:012x}")
    }

    /// The values as a decoded byte-array chunk.
    fn cells(values: &[String]) -> Vec<Option<ByteArray>> {
        values
            .iter()
            .map(|value| Some(ByteArray::from(value.clone().into_bytes())))
            .collect()
    }

    /// Check one rule's finding on `sample`, then check the suggestion it
    /// makes: every remedy names an engine and a setting, and the record
    /// round-trips through JSON with its remedies intact.
    fn check(rule_id: &'static str, sample: &ColumnSample<'_>, engines: &mut BTreeSet<String>) {
        let rule = rule(rule_id).expect("the rule exists");
        let finding = (rule.detect)(sample).expect("the sample earns the rule");
        assert!(!finding.remedies.is_empty(), "{rule_id} has no remedies");
        let mut named: Vec<String> = Vec::new();
        for remedy in &finding.remedies {
            assert!(
                !remedy.engine.is_empty(),
                "{rule_id} has a remedy with no engine"
            );
            assert!(
                !remedy.setting.is_empty(),
                "{rule_id}'s {} remedy has no setting",
                remedy.engine
            );
            assert!(
                !named.contains(&remedy.engine),
                "{rule_id} names {} twice",
                remedy.engine
            );
            named.push(remedy.engine.clone());
            engines.insert(remedy.engine.clone());
        }
        let suggestion = sample.candidate(rule_id, finding);
        assert_eq!(suggestion.remedies.len(), named.len(), "{rule_id}");
        let json = serde_json::to_string(&suggestion).expect("a suggestion serializes");
        let read: v1::Suggestion = serde_json::from_str(&json).expect("a suggestion parses");
        assert_eq!(read, suggestion, "{rule_id}");
    }

    /// Every rule carries structured remedies, and together they name every
    /// engine a table is compacted with.
    #[test]
    fn every_rule_populates_remedies_across_engines() {
        let mut engines = BTreeSet::new();

        let uuid_column = byte_column("UNCOMPRESSED");
        let uuids: Vec<String> = (0..100).map(uuid_text).collect();
        let uuid_cells = cells(&uuids);
        check(
            UUID_TEXT_RULE,
            &ColumnSample::for_test(&uuid_column, &uuid_cells, false),
            &mut engines,
        );

        let int96_column = typed_column("INT96", "UNCOMPRESSED");
        check(
            INT96_TIMESTAMP_RULE,
            &ColumnSample::for_test(&int96_column, &[], false),
            &mut engines,
        );

        let snappy_column = byte_column("SNAPPY");
        check(
            CODEC_ZSTD_RULE,
            &ColumnSample::for_test(&snappy_column, &[], false),
            &mut engines,
        );

        let unique_column = byte_column("ZSTD");
        let distinct: Vec<String> = (0..100).map(|value| format!("{value:016x}")).collect();
        let distinct_cells = cells(&distinct);
        check(
            DICTIONARY_UNIQUE_RULE,
            &ColumnSample::for_test(&unique_column, &distinct_cells, true),
            &mut engines,
        );

        let repeated: Vec<String> = (0..100)
            .map(|value| format!("region-{:02}", value % 5))
            .collect();
        let repeated_cells = cells(&repeated);
        check(
            DICTIONARY_MISSING_RULE,
            &ColumnSample::for_test(&unique_column, &repeated_cells, false),
            &mut engines,
        );

        check(
            DELTA_TEXT_ENCODING_RULE,
            &ColumnSample::for_test(&unique_column, &distinct_cells, false),
            &mut engines,
        );

        for engine in ["spark", "pyarrow", "parquet-rs", "delta", "iceberg"] {
            assert!(engines.contains(engine), "no rule remedies {engine}");
        }
    }

    /// A suggestion written without remedies reads as one that has none, and
    /// one with them keeps them: the field is additive on the wire.
    #[test]
    fn remedies_are_optional_on_the_wire() {
        let minimal: v1::Suggestion =
            serde_json::from_str(r#"{"name":"blobs/file/x/columns/id/suggestions/uuid-text","rule":"uuid-text","column":"id","message":"m","remedy":"r"}"#)
                .expect("a suggestion without remedies parses");
        assert!(minimal.remedies.is_empty());
        assert!(
            !serde_json::to_string(&minimal)
                .expect("serializes")
                .contains("remedies"),
            "an empty remedies list is not written"
        );

        let with_remedies = v1::Suggestion {
            remedies: vec![v1::Remedy {
                engine: "pyarrow".into(),
                setting: "column_types={id: pa.uuid()}".into(),
            }],
            ..minimal
        };
        let json = serde_json::to_string(&with_remedies).expect("serializes");
        assert_eq!(
            serde_json::from_str::<v1::Suggestion>(&json).expect("parses"),
            with_remedies
        );
    }

    /// A footer with these row-group row counts and no columns.
    fn metadata(rows_per_group: &[i64]) -> ParquetMetaData {
        let schema = Arc::new(Type::group_type_builder("schema").build().expect("schema"));
        let descr = Arc::new(SchemaDescriptor::new(schema));
        let row_groups = rows_per_group
            .iter()
            .map(|rows| {
                RowGroupMetaData::builder(descr.clone())
                    .set_num_rows(*rows)
                    .build()
                    .expect("row group")
            })
            .collect();
        let file = FileMetaData::new(1, rows_per_group.iter().sum(), None, None, descr, None);
        ParquetMetaData::new(file, row_groups)
    }

    #[test]
    fn the_representative_row_group_is_the_one_closest_to_average() {
        assert_eq!(representative_row_group(&metadata(&[100])), Some(0));
        assert_eq!(representative_row_group(&metadata(&[100, 100, 3])), Some(0));
        assert_eq!(representative_row_group(&metadata(&[3, 100, 100])), Some(1));
        // 25 rows in groups of 10, 10, 5: the full groups stand for the file.
        assert_eq!(representative_row_group(&metadata(&[10, 10, 5])), Some(0));
        assert_eq!(representative_row_group(&metadata(&[])), None);
    }

    /// Equally average groups break the tie by the lowest index.
    #[test]
    fn equal_groups_pick_the_first() {
        assert_eq!(representative_row_group(&metadata(&[7, 7, 7, 7])), Some(0));
    }
}
