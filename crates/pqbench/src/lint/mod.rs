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
//! rest. The decode reads the values of every physical type a rule re-encodes,
//! `BOOLEAN` and `INT96` excepted: `BOOLEAN` has no rule, and the `INT96` rule
//! reads its own timestamps. A codec suggestion rereads the chunk's own pages.
//!
//! `stats-truncated` is the one rule that can prove its saving arithmetically
//! instead of by a re-encode: a footer's `min_value` and `max_value` are the
//! writer's own bytes, so the bytes above the 64-byte truncation length are
//! exact without reading a page.
//!
//! Two rules read no file at all: `tombstones` and `small-files` decide from
//! the totals `list partitions` folded, so they hold for every partition,
//! including the ones a host with little network never copies.
//! [`partition_suggestions`] is that pair.

mod codec;
mod control;
mod decode;
mod delta_integer;
mod delta_text;
mod dictionary;
mod dictionary_fallback;
mod int96;
mod partition;
mod price;
mod stats;
mod text_timestamp;
mod uuid;
mod writer;

use std::collections::HashSet;
use std::fmt;
use std::hash::Hash;
use std::path::Path;

use parquet::data_type::ByteArray;

use crate::third_party::parquet::api::Error as ParquetError;
use crate::v1;

use self::control::Measurement;
use self::decode::{decode_leaves, pages_fell_back, Coverage, DecodedLeaves};
use self::price::Values;
use self::writer::Writer;

pub use codec::CODEC_ZSTD_RULE;
pub use delta_integer::DELTA_INTEGER_RULE;
pub use delta_text::DELTA_TEXT_ENCODING_RULE;
pub use dictionary::{DICTIONARY_MISSING_RULE, DICTIONARY_UNIQUE_RULE};
pub use dictionary_fallback::DICTIONARY_FALLBACK_RULE;
pub use int96::INT96_TIMESTAMP_RULE;
pub use partition::{partition_suggestions, SMALL_FILES_RULE, TOMBSTONES_RULE};
pub use stats::STATS_TRUNCATED_RULE;
pub use text_timestamp::TEXT_TIMESTAMP_RULE;
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
    /// The representative row group's whole chunk, one cell per row, in the
    /// vector of the leaf's physical type; every vector is empty for a leaf no
    /// rule re-encodes (`BOOLEAN`, `INT96`). A byte leaf fills `bytes`, a
    /// `FIXED_LEN_BYTE_ARRAY` one too, and each numeric leaf its own.
    bytes: &'a [Option<ByteArray>],
    int32_values: &'a [Option<i32>],
    int64_values: &'a [Option<i64>],
    float_values: &'a [Option<f32>],
    double_values: &'a [Option<f64>],
    /// A `FIXED_LEN_BYTE_ARRAY` leaf's value width.
    fixed_len: Option<i32>,
    /// The sample chunk's real on-disk compressed bytes.
    chunk_compressed_bytes: u64,
    /// The sample chunk wrote a dictionary page.
    chunk_has_dictionary: bool,
    /// Bytes from the sample chunk's dictionary page offset to its first data
    /// page.
    chunk_dictionary_page_size: Option<usize>,
    /// The sample chunk's data pages include PLAIN beside a dictionary
    /// encoding; `None` when the footer records no page encoding stats.
    chunk_fell_back: Option<bool>,
    /// Bytes the sample chunk's recorded `min_value` and `max_value` hold
    /// beyond the 64-byte truncation length.
    stats_excess_bytes: u64,
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
                None => {
                    let rows = chunk
                        .bytes
                        .len()
                        .max(chunk.int32_values.len())
                        .max(chunk.int64_values.len())
                        .max(chunk.float_values.len())
                        .max(chunk.double_values.len());
                    if rows == 0 {
                        value_count
                    } else {
                        let fraction = non_null as f64 / rows as f64;
                        (value_count as f64 * fraction).round() as u64
                    }
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
            bytes: chunk.bytes.as_slice(),
            int32_values: chunk.int32_values.as_slice(),
            int64_values: chunk.int64_values.as_slice(),
            float_values: chunk.float_values.as_slice(),
            double_values: chunk.double_values.as_slice(),
            fixed_len: chunk.fixed_len,
            chunk_compressed_bytes,
            chunk_has_dictionary: chunk.dictionary.present,
            chunk_dictionary_page_size: chunk.dictionary.page_size,
            chunk_fell_back: chunk.dictionary.fell_back,
            stats_excess_bytes: chunk.stats_excess_bytes,
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

    /// The leaf's physical type, as the footer spells it.
    fn physical(&self) -> &str {
        self.column
            .physical_data_type
            .as_deref()
            .unwrap_or_default()
    }

    /// The values one arm writes, in the leaf's physical shape. `None` for a
    /// leaf no rule re-encodes, so a rule that needs values never prices one.
    fn values(&self) -> Option<Values<'a>> {
        match self.physical() {
            "BYTE_ARRAY" if !self.bytes.is_empty() => Some(Values::Bytes(self.bytes)),
            "FIXED_LEN_BYTE_ARRAY" if !self.bytes.is_empty() => self
                .fixed_len
                .map(|width| Values::FixedLen(width, self.bytes)),
            "INT32" if !self.int32_values.is_empty() => Some(Values::Int32(self.int32_values)),
            "INT64" if !self.int64_values.is_empty() => Some(Values::Int64(self.int64_values)),
            "FLOAT" if !self.float_values.is_empty() => Some(Values::Float(self.float_values)),
            "DOUBLE" if !self.double_values.is_empty() => Some(Values::Double(self.double_values)),
            _ => None,
        }
    }

    /// The decoded byte cells: a byte leaf's values, as the text rules read
    /// them.
    fn cells(&self) -> &'a [Option<ByteArray>] {
        self.bytes
    }

    /// The leaf's values as `i64`, for the integer rules: an `INT32` widens.
    fn integers(&self) -> Option<Vec<Option<i64>>> {
        if !self.int32_values.is_empty() {
            Some(
                self.int32_values
                    .iter()
                    .map(|cell| cell.map(i64::from))
                    .collect(),
            )
        } else if !self.int64_values.is_empty() {
            Some(self.int64_values.to_vec())
        } else {
            None
        }
    }

    /// The non-null values the sample decoded, in the one vector the leaf's
    /// physical type fills; 0 for a leaf no rule re-encodes.
    fn sampled_values(&self) -> usize {
        [
            self.bytes.iter().filter(|cell| cell.is_some()).count(),
            self.int32_values
                .iter()
                .filter(|cell| cell.is_some())
                .count(),
            self.int64_values
                .iter()
                .filter(|cell| cell.is_some())
                .count(),
            self.float_values
                .iter()
                .filter(|cell| cell.is_some())
                .count(),
            self.double_values
                .iter()
                .filter(|cell| cell.is_some())
                .count(),
        ]
        .into_iter()
        .max()
        .unwrap_or(0)
    }

    /// The fraction of sampled non-null values that are distinct within the
    /// chunk (Parquet builds one dictionary per row-group column chunk).
    fn distinct_fraction(&self) -> f64 {
        if !self.bytes.is_empty() {
            distinct_share(
                self.bytes
                    .iter()
                    .map(|cell| cell.as_ref().map(ByteArray::data)),
            )
        } else if !self.int32_values.is_empty() {
            distinct_share(self.int32_values.iter().map(Option::as_ref))
        } else if !self.int64_values.is_empty() {
            distinct_share(self.int64_values.iter().map(Option::as_ref))
        } else if !self.float_values.is_empty() {
            // Two NaNs are one value and `-0.0` equals `0.0` at the physical
            // width, so the bits are what a dictionary would hold.
            distinct_share(self.float_values.iter().map(|cell| cell.map(f32::to_bits)))
        } else if !self.double_values.is_empty() {
            distinct_share(self.double_values.iter().map(|cell| cell.map(f64::to_bits)))
        } else {
            0.0
        }
    }

    /// Whether the sample chunk's own data pages fell back to PLAIN beside a
    /// dictionary. The footer's page encoding stats answer without a page read;
    /// a footer that counts no page encodings leaves it to the page headers.
    fn fell_back(&self) -> bool {
        if let Some(fell_back) = self.chunk_fell_back {
            return fell_back;
        }
        pages_fell_back(
            self.path,
            self.row_group,
            self.column_index,
            self.chunk_has_dictionary,
        )
        .unwrap_or(false)
    }

    /// The dictionary page size limit that reproduces the sample chunk's own
    /// behavior. A chunk that wrote a dictionary page and never fell back must
    /// not fall back in the control either, or the arms would compare a
    /// dictionary against PLAIN and credit a rewrite with the fallback's loss; a
    /// chunk that did fall back hit its writer's limit, which the dictionary
    /// page it had written by then approximates. `None` keeps parquet-rs's
    /// 1 MiB default.
    fn dictionary_page_size_limit(&self) -> Option<usize> {
        match (self.chunk_has_dictionary, self.fell_back()) {
            (true, false) => Some(usize::MAX),
            (true, true) => self.chunk_dictionary_page_size.filter(|bytes| *bytes > 0),
            (false, _) => None,
        }
    }

    /// The writer this file came from, as the rules' remedies need it.
    fn writer_kind(&self) -> Writer {
        Writer::classify(self.writer)
    }

    /// A sample over one chunk of a byte leaf, for the rules' unit tests.
    #[cfg(test)]
    pub(crate) fn for_test(
        column: &'a v1::Column,
        cells: &'a [Option<ByteArray>],
        chunk_has_dictionary: bool,
    ) -> Self {
        let mut sample = Self::build(column, cells.len() as u64);
        sample.bytes = cells;
        sample.chunk_has_dictionary = chunk_has_dictionary;
        sample
    }

    /// A sample over one chunk of an `INT32` leaf, for the rules' unit tests.
    #[cfg(test)]
    pub(crate) fn for_test_int32(
        column: &'a v1::Column,
        cells: &'a [Option<i32>],
        chunk_has_dictionary: bool,
    ) -> Self {
        let mut sample = Self::build(column, cells.len() as u64);
        sample.int32_values = cells;
        sample.chunk_has_dictionary = chunk_has_dictionary;
        sample
    }

    /// A sample over one chunk of an `INT64` leaf, for the rules' unit tests.
    #[cfg(test)]
    pub(crate) fn for_test_int64(
        column: &'a v1::Column,
        cells: &'a [Option<i64>],
        chunk_has_dictionary: bool,
    ) -> Self {
        let mut sample = Self::build(column, cells.len() as u64);
        sample.int64_values = cells;
        sample.chunk_has_dictionary = chunk_has_dictionary;
        sample
    }

    /// A sample over one chunk of a `FLOAT` leaf, for the rules' unit tests.
    #[cfg(test)]
    pub(crate) fn for_test_float(
        column: &'a v1::Column,
        cells: &'a [Option<f32>],
        chunk_has_dictionary: bool,
    ) -> Self {
        let mut sample = Self::build(column, cells.len() as u64);
        sample.float_values = cells;
        sample.chunk_has_dictionary = chunk_has_dictionary;
        sample
    }

    /// A sample over one chunk of a `DOUBLE` leaf, for the rules' unit tests.
    #[cfg(test)]
    pub(crate) fn for_test_double(
        column: &'a v1::Column,
        cells: &'a [Option<f64>],
        chunk_has_dictionary: bool,
    ) -> Self {
        let mut sample = Self::build(column, cells.len() as u64);
        sample.double_values = cells;
        sample.chunk_has_dictionary = chunk_has_dictionary;
        sample
    }

    /// A test sample's shared fields: no file, one chunk, parquet-rs's writer.
    #[cfg(test)]
    fn build(column: &'a v1::Column, rows: u64) -> Self {
        let mass = column.mass.as_ref().expect("a sample column has a mass");
        let chunk_compressed_bytes = mass.compressed_bytes.unwrap_or(0);
        Self {
            blob: "blobs/s3/example-bucket/data.parquet",
            leaf: "id",
            column,
            mass,
            path: Path::new(""),
            row_group: 0,
            column_index: 0,
            bytes: &[],
            int32_values: &[],
            int64_values: &[],
            float_values: &[],
            double_values: &[],
            fixed_len: column
                .physical_data_type
                .as_deref()
                .filter(|physical| *physical == "FIXED_LEN_BYTE_ARRAY")
                .map(|_| 16),
            chunk_compressed_bytes,
            chunk_has_dictionary: false,
            chunk_dictionary_page_size: None,
            chunk_fell_back: Some(false),
            stats_excess_bytes: 0,
            writer: Some("parquet-rs version 59.3.2"),
            coverage: Coverage {
                row_count: rows,
                row_group_count: 1,
            },
            text_bpv: chunk_compressed_bytes as f64 / rows.max(1) as f64,
        }
    }

    /// The same test sample, with a chunk that wrote a dictionary page of
    /// `page_size` bytes and fell back to `PLAIN` data pages.
    #[cfg(test)]
    pub(crate) fn with_dictionary_page(mut self, page_size: usize) -> Self {
        self.chunk_has_dictionary = true;
        self.chunk_dictionary_page_size = Some(page_size);
        self.chunk_fell_back = Some(true);
        self
    }

    /// The same test sample, with a chunk whose recorded bounds hold
    /// `bytes` past the truncation length.
    #[cfg(test)]
    pub(crate) fn with_stats_excess(mut self, bytes: u64) -> Self {
        self.stats_excess_bytes = bytes;
        self
    }

    /// The same test sample, with a chunk the footer prices at `bytes`.
    #[cfg(test)]
    pub(crate) fn with_chunk_compressed_bytes(mut self, bytes: u64) -> Self {
        self.chunk_compressed_bytes = bytes;
        self
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

/// The fraction of non-null values that are distinct.
fn distinct_share<T: Eq + Hash>(values: impl Iterator<Item = Option<T>>) -> f64 {
    let mut distinct = HashSet::new();
    let mut total = 0usize;
    for value in values {
        let Some(value) = value else { continue };
        total += 1;
        distinct.insert(value);
    }
    distinct.len() as f64 / total.max(1) as f64
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
/// its columns fall through to the encoding rules. The rules that change a
/// column's type (`uuid-text`, `text-timestamp`) outrank it, because a value
/// shrunk to a sixteenth of its bytes beats any codec; `stats-truncated` comes
/// last, because its few hundred footer bytes are the smallest win a column can
/// have. `delta-integer` outranks the dictionary rules because a dense integer
/// column's dictionary is only present when it overflowed, and packed deltas it
/// never uses are the larger win.
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
        id: TEXT_TIMESTAMP_RULE,
        detect: text_timestamp::detect,
    },
    Rule {
        id: CODEC_ZSTD_RULE,
        detect: codec::detect,
    },
    Rule {
        id: DELTA_INTEGER_RULE,
        detect: delta_integer::detect,
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
    Rule {
        id: DICTIONARY_FALLBACK_RULE,
        detect: dictionary_fallback::detect,
    },
    Rule {
        id: STATS_TRUNCATED_RULE,
        detect: stats::detect,
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use parquet::data_type::ByteArray;

    use super::{
        rule, ColumnSample, CODEC_ZSTD_RULE, DELTA_INTEGER_RULE, DELTA_TEXT_ENCODING_RULE,
        DICTIONARY_FALLBACK_RULE, DICTIONARY_MISSING_RULE, DICTIONARY_UNIQUE_RULE,
        INT96_TIMESTAMP_RULE, STATS_TRUNCATED_RULE, TEXT_TIMESTAMP_RULE, UUID_TEXT_RULE,
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

        let dense_column = typed_column("INT64", "ZSTD");
        let dense: Vec<Option<i64>> = (0..100).map(|value| Some(i64::from(value))).collect();
        check(
            DELTA_INTEGER_RULE,
            &ColumnSample::for_test_int64(&dense_column, &dense, false),
            &mut engines,
        );

        check(
            DICTIONARY_FALLBACK_RULE,
            &ColumnSample::for_test(&unique_column, &distinct_cells, true)
                .with_dictionary_page(4_096),
            &mut engines,
        );

        check(
            STATS_TRUNCATED_RULE,
            &ColumnSample::for_test(&unique_column, &distinct_cells, false)
                .with_chunk_compressed_bytes(100_000)
                .with_stats_excess(1_024),
            &mut engines,
        );

        let stamps: Vec<String> = (0..100)
            .map(|value| format!("2024-02-29T12:34:{:02}Z", value % 60))
            .collect();
        let stamp_cells = cells(&stamps);
        check(
            TEXT_TIMESTAMP_RULE,
            &ColumnSample::for_test(&unique_column, &stamp_cells, false),
            &mut engines,
        );

        for engine in [
            "spark",
            "pyarrow",
            "parquet-rs",
            "delta",
            "iceberg",
            "duckdb",
        ] {
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
}
