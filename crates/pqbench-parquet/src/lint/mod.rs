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
//! A leaf's repetition level decides which rules may read it. A rule that
//! decodes values (`uuid-text`, `text-timestamp`, `numeric-text`,
//! `base64-text`, the narrowing, Delta and dictionary rules, `page-size`) runs
//! on scalar leaves only: a repeated leaf's values are nested, and re-encoding
//! them is not implemented. A rule that reads page headers, raw page bytes, or
//! footer metadata (`codec-zstd`, `stats-truncated`) runs on any leaf, so a
//! nested `imp[]` or `seatbid[].bid[]` column is priced like a scalar one.
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

mod base64;
mod codec;
mod control;
mod decimal_narrow;
mod decode;
mod delta_integer;
mod delta_text;
mod dictionary;
mod dictionary_fallback;
mod int64_narrow;
mod int96;
mod numeric_text;
mod page_size;
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

use parquet::basic::Encoding;
use parquet::data_type::ByteArray;

use crate::third_party::parquet::api::Error as ParquetError;
use crate::v1;

use self::control::Measurement;
use self::decode::{decode_leaves, pages_fell_back, Coverage, DecodedLeaves};
use self::price::{Settings, Values};
use self::writer::Writer;

pub use base64::BASE64_TEXT_RULE;
pub use codec::CODEC_ZSTD_RULE;
pub use decimal_narrow::DECIMAL_NARROW_RULE;
pub use delta_integer::DELTA_INTEGER_RULE;
pub use delta_text::DELTA_TEXT_ENCODING_RULE;
pub use dictionary::{DICTIONARY_MISSING_RULE, DICTIONARY_UNIQUE_RULE};
pub use dictionary_fallback::DICTIONARY_FALLBACK_RULE;
pub use int64_narrow::INT64_NARROW_RULE;
pub use int96::INT96_TIMESTAMP_RULE;
pub use numeric_text::NUMERIC_TEXT_RULE;
pub use page_size::PAGE_SIZE_RULE;
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

/// The sample chunk's decoded cells, in the one vector the leaf's physical
/// type fills. A leaf no rule re-encodes (`BOOLEAN`, `INT96`) has none.
#[derive(Clone, Copy)]
pub(super) enum ColumnValues<'a> {
    /// A leaf no rule re-encodes.
    None,
    /// A `BYTE_ARRAY` or `FIXED_LEN_BYTE_ARRAY` leaf's cells.
    ByteArray(&'a [Option<ByteArray>]),
    /// An `INT32` leaf's cells.
    Int32(&'a [Option<i32>]),
    /// An `INT64` leaf's cells.
    Int64(&'a [Option<i64>]),
    /// A `FLOAT` leaf's cells.
    Float(&'a [Option<f32>]),
    /// A `DOUBLE` leaf's cells.
    Double(&'a [Option<f64>]),
}

impl ColumnValues<'_> {
    /// The cells the chunk decoded, nulls included.
    fn len(self) -> usize {
        match self {
            Self::None => 0,
            Self::ByteArray(cells) => cells.len(),
            Self::Int32(cells) => cells.len(),
            Self::Int64(cells) => cells.len(),
            Self::Float(cells) => cells.len(),
            Self::Double(cells) => cells.len(),
        }
    }

    /// The non-null cells the chunk decoded.
    fn non_null(self) -> usize {
        match self {
            Self::None => 0,
            Self::ByteArray(cells) => cells.iter().filter(|cell| cell.is_some()).count(),
            Self::Int32(cells) => cells.iter().filter(|cell| cell.is_some()).count(),
            Self::Int64(cells) => cells.iter().filter(|cell| cell.is_some()).count(),
            Self::Float(cells) => cells.iter().filter(|cell| cell.is_some()).count(),
            Self::Double(cells) => cells.iter().filter(|cell| cell.is_some()).count(),
        }
    }

    /// The fraction of the non-null cells that are distinct.
    fn distinct_fraction(self) -> f64 {
        match self {
            Self::None => 0.0,
            Self::ByteArray(cells) => {
                distinct_share(cells.iter().map(|cell| cell.as_ref().map(ByteArray::data)))
            }
            Self::Int32(cells) => distinct_share(cells.iter().map(Option::as_ref)),
            Self::Int64(cells) => distinct_share(cells.iter().map(Option::as_ref)),
            // Two NaNs are one value and `-0.0` equals `0.0` at the physical
            // width, so the bits are what a dictionary would hold.
            Self::Float(cells) => distinct_share(cells.iter().map(|cell| cell.map(f32::to_bits))),
            Self::Double(cells) => distinct_share(cells.iter().map(|cell| cell.map(f64::to_bits))),
        }
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
    /// vector of the leaf's physical type; empty for a leaf no rule
    /// re-encodes (`BOOLEAN`, `INT96`). The name follows its type.
    // aipnaming: allow(aip-140/plural)
    values: ColumnValues<'a>,
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
                    let rows = chunk.values().len();
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
            values: chunk.values(),
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
        match self.values {
            ColumnValues::ByteArray(cells) if !cells.is_empty() => match self.physical() {
                "BYTE_ARRAY" => Some(Values::Bytes(cells)),
                "FIXED_LEN_BYTE_ARRAY" => {
                    self.fixed_len.map(|width| Values::FixedLen(width, cells))
                }
                _ => None,
            },
            ColumnValues::Int32(cells) if !cells.is_empty() => Some(Values::Int32(cells)),
            ColumnValues::Int64(cells) if !cells.is_empty() => Some(Values::Int64(cells)),
            ColumnValues::Float(cells) if !cells.is_empty() => Some(Values::Float(cells)),
            ColumnValues::Double(cells) if !cells.is_empty() => Some(Values::Double(cells)),
            _ => None,
        }
    }

    /// The decoded byte cells: a byte leaf's values, as the text rules read
    /// them.
    fn cells(&self) -> &'a [Option<ByteArray>] {
        match self.values {
            ColumnValues::ByteArray(cells) => cells,
            _ => &[],
        }
    }

    /// The leaf's `INT64` values, as the narrowing and Delta rules read them.
    fn int64s(&self) -> Option<&'a [Option<i64>]> {
        match self.values {
            ColumnValues::Int64(cells) => Some(cells),
            _ => None,
        }
    }

    /// The leaf's values as `i64`, for the integer rules: an `INT32` widens.
    fn integers(&self) -> Option<Vec<Option<i64>>> {
        match self.values {
            ColumnValues::Int32(cells) if !cells.is_empty() => {
                Some(cells.iter().map(|cell| cell.map(i64::from)).collect())
            }
            ColumnValues::Int64(cells) if !cells.is_empty() => Some(cells.to_vec()),
            _ => None,
        }
    }

    /// The non-null values the sample decoded; 0 for a leaf no rule
    /// re-encodes.
    fn sampled_values(&self) -> usize {
        self.values.non_null()
    }

    /// The fraction of sampled non-null values that are distinct within the
    /// chunk (Parquet builds one dictionary per row-group column chunk).
    fn distinct_fraction(&self) -> f64 {
        self.values.distinct_fraction()
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

    /// The settings that reproduce this chunk's own storage: its dictionary
    /// state and page limit, the recorded `encoding`, and the 1.0 page version
    /// a footer without one records.
    fn source_settings(&self, encoding: Option<Encoding>) -> Settings {
        Settings {
            dictionary: self.chunk_has_dictionary,
            dictionary_page_size_limit: self.dictionary_page_size_limit(),
            data_page_size_limit: None,
            encoding,
            version: parquet::file::properties::WriterVersion::PARQUET_1_0,
        }
    }

    /// A sample over one chunk, for the rules' unit tests.
    #[cfg(test)]
    pub(crate) fn for_test_values(
        column: &'a v1::Column,
        values: ColumnValues<'a>,
        chunk_has_dictionary: bool,
    ) -> Self {
        let mut sample = Self::build(column, values.len() as u64);
        sample.values = values;
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
            values: ColumnValues::None,
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
        let remedy = remedy_text(
            self.writer_kind(),
            self.leaf,
            &finding.remedies,
            &finding.advice,
        );
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
            remedy,
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
    /// The same advice as one setting per engine, so a reader can apply it in
    /// whichever engine compacts the table.
    pub(crate) remedies: Vec<v1::Remedy>,
    /// How the one-line remedy reads in each writer's terms.
    pub(crate) advice: RemedyAdvice,
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

/// `leaf` as a Python string literal: a pyarrow setting takes its column keys
/// as a dict literal, so a dotted, colon-bearing, or `@` name is quoted, and a
/// backslash or quote inside one is escaped.
pub(crate) fn py_string(leaf: &str) -> String {
    let mut literal = String::with_capacity(leaf.len() + 2);
    literal.push('\'');
    for ch in leaf.chars() {
        if matches!(ch, '\\' | '\'') {
            literal.push('\\');
        }
        literal.push(ch);
    }
    literal.push('\'');
    literal
}

/// A rule's setting for `engine`, from its remedy table: empty when the engine
/// has none.
pub(crate) fn setting(remedies: &[v1::Remedy], engine: &str) -> String {
    remedies
        .iter()
        .find(|remedy| remedy.engine == engine)
        .map_or_else(String::new, |remedy| remedy.setting.clone())
}

/// How one rule's one-line remedy reads in each writer's terms.
///
/// The rule's remedy table already names a setting per engine; this picks the
/// one the file's writer actually has and wraps it in the rule's own wording.
/// A wording is a template: `{lead}` is the rule's lead clause, `{leaf}` the
/// column name, and `{setting}` the writer's own setting.
#[derive(Clone, Copy)]
pub(crate) struct RemedyAdvice {
    /// The clause the writer's setting follows; empty when it stands alone.
    lead: &'static str,
    /// The engine Photon forwards (unverified): `delta` for a file-level
    /// setting, `spark` for a per-column one.
    photon: &'static str,
    /// The engine a writer this crate does not classify takes.
    other: &'static str,
    /// A writer whose remedy is not `lead` + its setting, and its wording.
    wordings: [Option<&'static str>; Writer::COUNT],
}

impl RemedyAdvice {
    /// The common shape: `lead` then the writer's setting, Photon forwarding
    /// `photon`'s key, and a writer this crate does not classify taking
    /// `iceberg`'s.
    pub(crate) const fn new(lead: &'static str, photon: &'static str) -> Self {
        Self {
            lead,
            photon,
            other: "iceberg",
            wordings: [None; Writer::COUNT],
        }
    }

    /// The same, with a writer this crate does not classify taking `engine`'s
    /// setting.
    pub(crate) const fn other(mut self, engine: &'static str) -> Self {
        self.other = engine;
        self
    }

    /// The same, with `writer`'s remedy spelled out.
    pub(crate) const fn wording(mut self, writer: Writer, wording: &'static str) -> Self {
        self.wordings[writer.index()] = Some(wording);
        self
    }

    /// A remedy that is the same sentence for every writer.
    pub(crate) const fn sentence(sentence: &'static str) -> Self {
        Self {
            lead: "",
            photon: "spark",
            other: "iceberg",
            wordings: [Some(sentence); Writer::COUNT],
        }
    }
}

/// The one-line remedy for `writer`, from the rule's advice and remedy table.
pub(super) fn remedy_text(
    writer: Writer,
    leaf: &str,
    remedies: &[v1::Remedy],
    advice: &RemedyAdvice,
) -> String {
    let engine = match writer {
        Writer::ParquetMr => "spark",
        Writer::Photon => advice.photon,
        Writer::ArrowCpp => "pyarrow",
        Writer::ArrowRs => "parquet-rs",
        Writer::Other => advice.other,
    };
    let setting = setting(remedies, engine);
    let default = match advice.lead.is_empty() {
        true => "{setting}",
        false => "{lead}: {setting}",
    };
    let wording = advice.wordings[writer.index()].unwrap_or(default);
    let line = fill(wording, advice.lead, leaf, &setting);
    match (writer, advice.wordings[writer.index()]) {
        // Photon forwards the parquet-mr keys, and whether it honors them is
        // unverified.
        (Writer::Photon, None) => format!("{line} (Photon forwarding unverified)"),
        _ => line,
    }
}

/// `template` with its keys replaced in one pass, so a column name that spells
/// one of them is not substituted a second time.
fn fill(template: &str, lead: &str, leaf: &str, setting: &str) -> String {
    let mut filled = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(at) = rest.find('{') {
        filled.push_str(&rest[..at]);
        rest = &rest[at..];
        if let Some(tail) = rest.strip_prefix("{lead}") {
            filled.push_str(lead);
            rest = tail;
        } else if let Some(tail) = rest.strip_prefix("{leaf}") {
            filled.push_str(leaf);
            rest = tail;
        } else if let Some(tail) = rest.strip_prefix("{setting}") {
            filled.push_str(setting);
            rest = tail;
        } else {
            filled.push('{');
            rest = &rest[1..];
        }
    }
    filled.push_str(rest);
    filled
}

/// A rule's controlled measurement of its rewrite.
pub(crate) type Measure = fn(&ColumnSample<'_>) -> Result<Option<Measurement>, Error>;

/// A rule: the detection that makes a column a candidate, and the control that
/// verifies it.
pub(crate) struct Rule {
    pub(crate) id: &'static str,
    /// The rule reads the leaf's decoded values. Such a rule runs on scalar
    /// leaves only, because a repeated leaf's values are nested; a rule that
    /// reads only page headers, raw page bytes, or footer facts runs on any
    /// leaf, repeated ones included.
    pub(crate) values: bool,
    pub(crate) detect: fn(&ColumnSample<'_>) -> Option<Finding>,
}

/// The rules, in priority order. The first that detects a column wins, so a
/// column's bytes are priced once and two rules never count the same saving.
///
/// `codec-zstd` outranks the encoding rules because its win is the larger one:
/// a snappy or uncompressed file's 35-60% codec upgrade must not be masked by a
/// 0-1% dictionary or Delta encoding tweak. A zstd file never reaches it, so
/// its columns fall through to the encoding rules. The rules that change a
/// column's type (`uuid-text`, `base64-text`, `numeric-text`, `text-timestamp`,
/// `int64-narrow`, `decimal-narrow`) outrank it, because a value stored at a
/// width or in a shape it does not need beats any codec; `stats-truncated`
/// comes last, because its few hundred footer bytes are the smallest win a
/// column can have. `delta-integer` outranks the narrowing rules because packed
/// deltas on a dense column beat a narrower plain width; the narrowing rules
/// outrank the dictionary rules because a dictionary is only what a writer
/// reaches for after the width is decided. `delta-text-encoding` follows the
/// dictionary rules — a dictionary is the writer's own answer to repeated
/// text — and prices both Delta encodings and the shared-prefix one.
/// `page-size` follows `codec-zstd`: a weaker codec is
/// the larger win, while a zstd chunk's tiny pages still earn it.
pub(crate) const RULES: &[Rule] = &[
    Rule {
        id: UUID_TEXT_RULE,
        values: true,
        detect: uuid::detect,
    },
    Rule {
        id: BASE64_TEXT_RULE,
        values: true,
        detect: base64::detect,
    },
    Rule {
        id: INT96_TIMESTAMP_RULE,
        values: true,
        detect: int96::detect,
    },
    Rule {
        id: TEXT_TIMESTAMP_RULE,
        values: true,
        detect: text_timestamp::detect,
    },
    Rule {
        id: NUMERIC_TEXT_RULE,
        values: true,
        detect: numeric_text::detect,
    },
    Rule {
        id: CODEC_ZSTD_RULE,
        values: false,
        detect: codec::detect,
    },
    Rule {
        id: PAGE_SIZE_RULE,
        values: true,
        detect: page_size::detect,
    },
    Rule {
        id: DELTA_INTEGER_RULE,
        values: true,
        detect: delta_integer::detect,
    },
    Rule {
        id: INT64_NARROW_RULE,
        values: true,
        detect: int64_narrow::detect,
    },
    Rule {
        id: DECIMAL_NARROW_RULE,
        values: true,
        detect: decimal_narrow::detect,
    },
    Rule {
        id: DICTIONARY_UNIQUE_RULE,
        values: true,
        detect: dictionary::detect_unique,
    },
    Rule {
        id: DICTIONARY_MISSING_RULE,
        values: true,
        detect: dictionary::detect_missing,
    },
    Rule {
        id: DELTA_TEXT_ENCODING_RULE,
        values: true,
        detect: delta_text::detect,
    },
    Rule {
        id: DICTIONARY_FALLBACK_RULE,
        values: true,
        detect: dictionary_fallback::detect,
    },
    Rule {
        id: STATS_TRUNCATED_RULE,
        values: false,
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
/// is measured. A repeated leaf is measured by the rules that read its pages
/// and footer, and never decoded.
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
            if rule.values && column.max_repetition_level != Some(0) {
                continue;
            }
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
    let rule = rule(&candidate.rule).ok_or_else(|| Error::Rule(candidate.rule.clone()))?;
    let column = measured
        .columns
        .iter()
        .find(|measured| {
            v1::Column::parse_name(&measured.name)
                .is_some_and(|(_, _, leaf)| leaf == candidate.column)
        })
        .ok_or_else(|| Error::Invalid(format!("the file has no column `{}`", candidate.column)))?;
    if rule.values && column.max_repetition_level != Some(0) {
        return Ok(reject(
            candidate,
            blob,
            None,
            "the column is repeated; this rule reads scalar values only",
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
    enriched.remedies = finding.remedies;
    enriched.remedy = remedy_text(
        Writer::classify(blob.writer.as_deref()),
        &enriched.column,
        &enriched.remedies,
        &finding.advice,
    );
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

/// Canonical UUID text for `value`, as the rule tests write it.
#[cfg(test)]
pub(crate) fn uuid_text(value: usize) -> String {
    format!("{value:08x}-0000-4000-8000-{value:012x}")
}

/// The values as a decoded byte-array chunk, as the rule tests write it.
#[cfg(test)]
pub(crate) fn cells(values: &[String]) -> Vec<Option<ByteArray>> {
    values
        .iter()
        .map(|value| Some(ByteArray::from(value.clone().into_bytes())))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::path::Path;
    use std::sync::Arc;

    use base64::engine::general_purpose::STANDARD;
    use base64::Engine as _;
    use parquet::basic::{Compression, ZstdLevel};
    use parquet::data_type::{ByteArray, ByteArrayType, Int64Type};
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::parser::parse_message_type;

    use super::{
        cells, remedy_text, rule, setting, suggestions, uuid_text, verify, ColumnSample,
        ColumnValues, RemedyAdvice, BASE64_TEXT_RULE, CODEC_ZSTD_RULE, DECIMAL_NARROW_RULE,
        DELTA_INTEGER_RULE, DELTA_TEXT_ENCODING_RULE, DICTIONARY_FALLBACK_RULE,
        DICTIONARY_MISSING_RULE, DICTIONARY_UNIQUE_RULE, INT64_NARROW_RULE, INT96_TIMESTAMP_RULE,
        NUMERIC_TEXT_RULE, STATS_TRUNCATED_RULE, TEXT_TIMESTAMP_RULE, UUID_TEXT_RULE,
    };
    use crate::lint::writer::Writer;
    use crate::v1;

    /// A `BYTE_ARRAY` column whose chunk is stored at `codec`.
    fn byte_column(codec: &str) -> v1::Column {
        v1::Column {
            physical_data_type: Some("BYTE_ARRAY".into()),
            max_repetition_level: Some(0),
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
            max_repetition_level: Some(0),
            mass: Some(v1::Bytemass {
                codec: Some(codec.into()),
                ..v1::Bytemass::default()
            }),
            ..v1::Column::default()
        }
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
        for writer in [
            Writer::Photon,
            Writer::ParquetMr,
            Writer::ArrowCpp,
            Writer::ArrowRs,
            Writer::Other,
        ] {
            let line = remedy_text(writer, sample.leaf, &finding.remedies, &finding.advice);
            assert!(!line.is_empty(), "{rule_id} has no {writer:?} remedy");
            for key in ["{lead}", "{leaf}", "{setting}"] {
                assert!(
                    !line.contains(key),
                    "{rule_id}'s {writer:?} remedy leaves `{key}`: {line}"
                );
            }
            if finding.advice.wordings[writer.index()].is_some() {
                continue;
            }
            let engine = match writer {
                Writer::ParquetMr => "spark",
                Writer::Photon => finding.advice.photon,
                Writer::ArrowCpp => "pyarrow",
                Writer::ArrowRs => "parquet-rs",
                Writer::Other => finding.advice.other,
            };
            let advice = setting(&finding.remedies, engine);
            assert!(
                line.contains(&advice),
                "{rule_id}'s {writer:?} remedy does not name `{advice}`: {line}"
            );
            if writer == Writer::Photon {
                assert!(
                    line.contains("unverified"),
                    "{rule_id}'s Photon remedy does not say so: {line}"
                );
            }
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
            &ColumnSample::for_test_values(
                &uuid_column,
                ColumnValues::ByteArray(&uuid_cells),
                false,
            ),
            &mut engines,
        );

        let base64_column = byte_column("UNCOMPRESSED");
        let payloads: Vec<String> = (0..100u32)
            .map(|value| {
                let raw: Vec<u8> = (0..96u32)
                    .map(|at| (value.wrapping_mul(31).wrapping_add(at * 7) % 251) as u8)
                    .collect();
                STANDARD.encode(&raw)
            })
            .collect();
        let payload_cells = cells(&payloads);
        check(
            BASE64_TEXT_RULE,
            &ColumnSample::for_test_values(
                &base64_column,
                ColumnValues::ByteArray(&payload_cells),
                false,
            ),
            &mut engines,
        );

        let int96_column = typed_column("INT96", "UNCOMPRESSED");
        check(
            INT96_TIMESTAMP_RULE,
            &ColumnSample::for_test_values(&int96_column, ColumnValues::ByteArray(&[]), false),
            &mut engines,
        );

        let numeric_column = byte_column("UNCOMPRESSED");
        let numbers: Vec<String> = (0..100)
            .map(|value| (100_001 + value).to_string())
            .collect();
        let number_cells = cells(&numbers);
        check(
            NUMERIC_TEXT_RULE,
            &ColumnSample::for_test_values(
                &numeric_column,
                ColumnValues::ByteArray(&number_cells),
                false,
            ),
            &mut engines,
        );

        let narrow_column = typed_column("INT64", "UNCOMPRESSED");
        let small: Vec<Option<i64>> = (0..100).map(|value| Some(value + 40_000)).collect();
        check(
            INT64_NARROW_RULE,
            &ColumnSample::for_test_values(&narrow_column, ColumnValues::Int64(&small), false),
            &mut engines,
        );

        let mut decimal_column = typed_column("FIXED_LEN_BYTE_ARRAY", "UNCOMPRESSED");
        decimal_column.mass = Some(v1::Bytemass {
            codec: Some("UNCOMPRESSED".into()),
            logical_type: Some("DECIMAL(9,2)".into()),
            ..v1::Bytemass::default()
        });
        let amounts: Vec<Option<ByteArray>> = (0..100)
            .map(|value| {
                Some(ByteArray::from(
                    (1_000 + value as i64).to_be_bytes().to_vec(),
                ))
            })
            .collect();
        check(
            DECIMAL_NARROW_RULE,
            &ColumnSample::for_test_values(
                &decimal_column,
                ColumnValues::ByteArray(&amounts),
                false,
            ),
            &mut engines,
        );

        let snappy_column = byte_column("SNAPPY");
        check(
            CODEC_ZSTD_RULE,
            &ColumnSample::for_test_values(&snappy_column, ColumnValues::ByteArray(&[]), false),
            &mut engines,
        );

        let unique_column = byte_column("ZSTD");
        let distinct: Vec<String> = (0..100).map(|value| format!("{value:016x}")).collect();
        let distinct_cells = cells(&distinct);
        check(
            DICTIONARY_UNIQUE_RULE,
            &ColumnSample::for_test_values(
                &unique_column,
                ColumnValues::ByteArray(&distinct_cells),
                true,
            ),
            &mut engines,
        );

        let repeated: Vec<String> = (0..100)
            .map(|value| format!("region-{:02}", value % 5))
            .collect();
        let repeated_cells = cells(&repeated);
        check(
            DICTIONARY_MISSING_RULE,
            &ColumnSample::for_test_values(
                &unique_column,
                ColumnValues::ByteArray(&repeated_cells),
                false,
            ),
            &mut engines,
        );

        check(
            DELTA_TEXT_ENCODING_RULE,
            &ColumnSample::for_test_values(
                &unique_column,
                ColumnValues::ByteArray(&distinct_cells),
                false,
            ),
            &mut engines,
        );

        let dense_column = typed_column("INT64", "ZSTD");
        let dense: Vec<Option<i64>> = (0..100).map(|value| Some(i64::from(value))).collect();
        check(
            DELTA_INTEGER_RULE,
            &ColumnSample::for_test_values(&dense_column, ColumnValues::Int64(&dense), false),
            &mut engines,
        );

        check(
            DICTIONARY_FALLBACK_RULE,
            &ColumnSample::for_test_values(
                &unique_column,
                ColumnValues::ByteArray(&distinct_cells),
                true,
            )
            .with_dictionary_page(4_096),
            &mut engines,
        );

        check(
            STATS_TRUNCATED_RULE,
            &ColumnSample::for_test_values(
                &unique_column,
                ColumnValues::ByteArray(&distinct_cells),
                false,
            )
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
            &ColumnSample::for_test_values(
                &unique_column,
                ColumnValues::ByteArray(&stamp_cells),
                false,
            ),
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

    /// The remedy helper wraps a writer's own setting in the rule's lead,
    /// spells out a writer whose wording differs, substitutes the leaf once,
    /// and marks Photon's forwarded key unverified.
    #[test]
    fn a_remedy_names_each_writer_setting_and_its_own_wording() {
        let remedies = vec![
            v1::Remedy::new("spark", "spark.setting"),
            v1::Remedy::new("delta", "delta.setting"),
            v1::Remedy::new("duckdb", "duckdb.setting"),
            v1::Remedy::new("pyarrow", "pyarrow.setting"),
            v1::Remedy::new("parquet-rs", "parquet-rs.setting"),
            v1::Remedy::new("iceberg", "iceberg.setting"),
        ];
        let advice = RemedyAdvice::new("do the thing", "delta")
            .wording(Writer::ArrowRs, "write {leaf} directly");
        assert_eq!(
            remedy_text(Writer::ParquetMr, "a.b", &remedies, &advice),
            "do the thing: spark.setting"
        );
        assert_eq!(
            remedy_text(Writer::Photon, "a.b", &remedies, &advice),
            "do the thing: delta.setting (Photon forwarding unverified)"
        );
        assert_eq!(
            remedy_text(Writer::ArrowCpp, "a.b", &remedies, &advice),
            "do the thing: pyarrow.setting"
        );
        assert_eq!(
            remedy_text(Writer::ArrowRs, "a.b", &remedies, &advice),
            "write a.b directly"
        );
        assert_eq!(
            remedy_text(Writer::Other, "a.b", &remedies, &advice),
            "do the thing: iceberg.setting"
        );

        let leadless = RemedyAdvice::new("", "spark").other("duckdb");
        assert_eq!(
            remedy_text(Writer::ParquetMr, "id", &remedies, &leadless),
            "spark.setting"
        );
        assert_eq!(
            remedy_text(Writer::Other, "id", &remedies, &leadless),
            "duckdb.setting"
        );

        // A leaf that spells a key is substituted once, not expanded twice.
        let templated = RemedyAdvice::new("", "spark").wording(Writer::ArrowRs, "write {leaf}");
        assert_eq!(
            remedy_text(Writer::ArrowRs, "{setting}", &remedies, &templated),
            "write {setting}"
        );

        let sentence = RemedyAdvice::sentence("store as BINARY(16)");
        for writer in [
            Writer::Photon,
            Writer::ParquetMr,
            Writer::ArrowCpp,
            Writer::ArrowRs,
            Writer::Other,
        ] {
            assert_eq!(
                remedy_text(writer, "id", &remedies, &sentence),
                "store as BINARY(16)"
            );
        }
    }

    /// A pyarrow setting takes its column key as a Python string literal, so
    /// the helper quotes every name and escapes the two characters that would
    /// end the literal early.
    #[test]
    fn a_python_string_literal_quotes_and_escapes() {
        for leaf in ["request.ext", "site.publisher:ext", "request.@timestamp"] {
            assert_eq!(super::py_string(leaf), format!("'{leaf}'"));
        }
        assert_eq!(super::py_string("a'b\\c"), "'a\\'b\\\\c'");
    }

    /// Write an OpenRTB-shaped bid stream: a scalar `id` and a repeated
    /// `imp` group whose `id` is UUID text and whose `ext` holds a long
    /// payload. `imp.id` is snappy-compressed (a codec candidate) and
    /// `imp.ext` is zstd-compressed with untruncated statistics (a footer
    /// candidate).
    fn write_bid_requests(path: &Path) {
        let schema = Arc::new(
            parse_message_type(
                "message bid_request {
                    REQUIRED INT64 id;
                    REPEATED GROUP imp {
                        REQUIRED BINARY id (STRING);
                        REQUIRED BINARY ext (STRING);
                    }
                }",
            )
            .expect("a valid schema"),
        );
        let properties = Arc::new(
            WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .set_column_compression(
                    vec!["imp".to_string(), "ext".to_string()].into(),
                    Compression::ZSTD(ZstdLevel::default()),
                )
                .set_statistics_truncate_length(None)
                .build(),
        );
        let output = std::fs::File::create(path).expect("create the fixture");
        let mut writer =
            SerializedFileWriter::new(output, schema, properties).expect("a parquet writer");
        let mut row_group = writer.next_row_group().expect("a row group");

        let rows = 2_000u64;
        let ids: Vec<i64> = (0..i64::try_from(rows).unwrap()).collect();
        let mut imp_ids = Vec::with_capacity(rows as usize * 2);
        let mut imp_ext = Vec::with_capacity(rows as usize * 2);
        let mut def_levels = Vec::with_capacity(rows as usize * 2);
        let mut rep_levels = Vec::with_capacity(rows as usize * 2);
        for row in 0..rows {
            for place in 0..2 {
                let uuid = format!("{row:08x}-0000-4000-8000-{row:012x}");
                imp_ids.push(ByteArray::from(uuid.into_bytes()));
                let payload = format!("{}-{}", "ext".repeat(100), row % 8);
                imp_ext.push(ByteArray::from(payload.into_bytes()));
                def_levels.push(1);
                rep_levels.push(i16::from(place > 0));
            }
        }

        let mut column = row_group.next_column().unwrap().expect("an id column");
        column
            .typed::<Int64Type>()
            .write_batch(&ids, None, None)
            .expect("write the ids");
        column.close().expect("close the ids");

        let mut column = row_group.next_column().unwrap().expect("an imp.id column");
        column
            .typed::<ByteArrayType>()
            .write_batch(&imp_ids, Some(&def_levels), Some(&rep_levels))
            .expect("write imp.id");
        column.close().expect("close imp.id");

        let mut column = row_group.next_column().unwrap().expect("an imp.ext column");
        column
            .typed::<ByteArrayType>()
            .write_batch(&imp_ext, Some(&def_levels), Some(&rep_levels))
            .expect("write imp.ext");
        column.close().expect("close imp.ext");

        row_group.close().expect("close the row group");
        writer.close().expect("close the file");
    }

    /// A repeated leaf is priced from its page headers, page bytes, and footer
    /// facts: `codec-zstd` and `stats-truncated` both detect it and both
    /// verify a non-zero saving, while the value-decoding rules stay out.
    #[test]
    fn a_repeated_leaf_is_priced_by_its_pages_and_footer() {
        let directory =
            std::env::temp_dir().join(format!("pqbench-lint-repeated-{}", std::process::id()));
        std::fs::create_dir_all(&directory).expect("a temporary directory");
        let path = directory.join("bid_requests.parquet");
        write_bid_requests(&path);

        let file = v1::TableFile {
            name: v1::name::table_file("local", "rtb", "bid_requests", "f1"),
            blob: Some(v1::name::blob_name(path.to_str().expect("a utf-8 path"))),
            ..v1::TableFile::default()
        };
        let candidates = suggestions(&path, &file).expect("lint the file");

        let codec = candidates
            .iter()
            .find(|candidate| candidate.rule == CODEC_ZSTD_RULE && candidate.column == "imp.id")
            .unwrap_or_else(|| panic!("no codec-zstd on the repeated leaf: {candidates:#?}"));
        let measured = verify(&path, codec).expect("verify the codec candidate");
        assert_eq!(measured.verified, Some(true), "{}", measured.message);
        assert!(
            measured.savings_bytes.unwrap_or(0) > 0,
            "{}",
            measured.message
        );

        let stats = candidates
            .iter()
            .find(|candidate| {
                candidate.rule == STATS_TRUNCATED_RULE && candidate.column == "imp.ext"
            })
            .unwrap_or_else(|| panic!("no stats-truncated on the repeated leaf: {candidates:#?}"));
        let measured = verify(&path, stats).expect("verify the footer candidate");
        assert_eq!(measured.verified, Some(true), "{}", measured.message);
        assert!(
            measured.savings_bytes.unwrap_or(0) > 0,
            "{}",
            measured.message
        );

        // The values are canonical UUID text, but the leaf repeats: a rule that
        // decodes values must not claim a repeated leaf.
        assert!(
            candidates
                .iter()
                .all(|candidate| candidate.rule != UUID_TEXT_RULE),
            "{candidates:#?}"
        );

        let _ = std::fs::remove_dir_all(directory);
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
