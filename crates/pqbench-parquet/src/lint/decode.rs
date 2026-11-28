//! `lint::decode`: the raw Parquet mechanism behind a lint sample.
//!
//! This module reads one file's footer and its representative row group; it
//! decodes cells for every physical type a rule prices, and page-reading rules
//! reread the raw pages from here. It decides nothing about what a column
//! should be: the rules ([`super::RULES`]) and the suggestion policy
//! ([`super::suggestions`], [`super::verify`]) work on the sample this module
//! reports.

use std::collections::HashMap;
use std::path::Path;

use parquet::basic::{Encoding, PageType};
use parquet::column::reader::{ColumnReader, ColumnReaderImpl};
use parquet::data_type::{ByteArray, DataType, FixedLenByteArrayType};
use parquet::file::metadata::{
    ColumnChunkMetaData, PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader,
};
use parquet::file::reader::{FileReader, RowGroupReader, SerializedFileReader};
use parquet::file::statistics::Statistics;
use parquet::schema::types::ColumnDescriptor;

use crate::third_party::parquet::api::Error as ParquetError;

use super::Error;

/// The bytes of a chunk's recorded `min_value` and `max_value` beyond the
/// 64-byte length every writer that truncates statistics uses.
const STATISTICS_TRUNCATE_LENGTH: u64 = 64;

/// What a sample decoded: rows and the whole row groups they came from.
#[derive(Clone, Copy, Default)]
pub(super) struct Coverage {
    pub(super) row_count: u64,
    pub(super) row_group_count: u64,
}

/// What a column chunk's footer records about its dictionary encoding.
#[derive(Clone, Copy, Default)]
pub(super) struct ChunkDictionary {
    /// The chunk is dictionary-encoded: a dictionary page offset, dictionary
    /// data pages, or a dictionary encoding in the chunk's encoding set.
    pub(super) present: bool,
    /// Bytes from the dictionary page offset to the first data page.
    pub(super) page_size: Option<usize>,
    /// The chunk's data pages include PLAIN beside a dictionary encoding, so
    /// its writer fell back to PLAIN. `None` when the footer records no page
    /// encoding stats: the page headers then have to say.
    pub(super) fell_back: Option<bool>,
}

/// One requested column's representative row group chunk.
#[derive(Default)]
pub(super) struct DecodedColumn {
    /// One cell per row of the group; `None` where the column is null. Only the
    /// vector of the leaf's physical type is filled.
    pub(super) bytes: Vec<Option<ByteArray>>,
    pub(super) int32_values: Vec<Option<i32>>,
    pub(super) int64_values: Vec<Option<i64>>,
    pub(super) float_values: Vec<Option<f32>>,
    pub(super) double_values: Vec<Option<f64>>,
    /// Non-null values in the chunk, exact when the cells were decoded and
    /// from the footer statistics otherwise.
    pub(super) non_null_value_count: u64,
    /// The chunk's compressed bytes.
    pub(super) compressed_bytes: u64,
    /// The chunk's dictionary page, as its footer records it.
    pub(super) dictionary: ChunkDictionary,
    /// The chunk's leaf index in the file schema.
    pub(super) column_index: usize,
    /// A `FIXED_LEN_BYTE_ARRAY` leaf's value width.
    pub(super) fixed_len: Option<i32>,
    /// Bytes the chunk's recorded `min_value` and `max_value` hold beyond the
    /// 64-byte truncation length.
    pub(super) stats_excess_bytes: u64,
}

impl DecodedColumn {
    /// The non-null values the decoded cells hold.
    fn decoded_non_null(&self) -> Option<u64> {
        if !self.bytes.is_empty() {
            Some(self.bytes.iter().filter(|cell| cell.is_some()).count() as u64)
        } else if !self.int32_values.is_empty() {
            Some(
                self.int32_values
                    .iter()
                    .filter(|cell| cell.is_some())
                    .count() as u64,
            )
        } else if !self.int64_values.is_empty() {
            Some(
                self.int64_values
                    .iter()
                    .filter(|cell| cell.is_some())
                    .count() as u64,
            )
        } else if !self.float_values.is_empty() {
            Some(
                self.float_values
                    .iter()
                    .filter(|cell| cell.is_some())
                    .count() as u64,
            )
        } else if !self.double_values.is_empty() {
            Some(
                self.double_values
                    .iter()
                    .filter(|cell| cell.is_some())
                    .count() as u64,
            )
        } else {
            None
        }
    }
}

/// The representative row group's whole chunks for the requested scalar leaves.
pub(super) struct DecodedLeaves {
    /// Per requested column: its chunk from the representative row group.
    pub(super) columns: Vec<DecodedColumn>,
    pub(super) coverage: Coverage,
    /// The representative row group's index, or zero when the file has none.
    pub(super) row_group: usize,
}

/// Decode the file's one representative row group for the requested leaf
/// columns, directly from their column chunks. Values are decoded for every
/// physical type a rule prices (`BOOLEAN` and `INT96` excluded: no rule
/// re-encodes them from this sample), and only for scalar leaves: a repeated
/// leaf's page and footer facts are recorded without decoding its nested
/// values, because the rules that read those facts (a codec, a footer bound)
/// need only the chunk's bytes. Unselected columns are never read.
pub(super) fn decode_leaves(path: &Path, leaves: &[String]) -> Result<DecodedLeaves, Error> {
    let file = std::fs::File::open(path).map_err(ParquetError::Io)?;
    let reader = SerializedFileReader::new(file)?;
    let metadata = reader.metadata();
    let schema = metadata.file_metadata().schema_descr();
    let leaf_index: HashMap<String, usize> = schema
        .columns()
        .iter()
        .enumerate()
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
        // A repeated leaf's values are nested, so no rule prices them: its
        // chunk keeps only the footer and page facts the window rules read.
        let mut column = if descr.max_rep_level() == 0 {
            decode_values(group.as_ref(), col_idx, &descr, row_count)?
        } else {
            DecodedColumn::default()
        };
        column.non_null_value_count = column
            .decoded_non_null()
            .unwrap_or_else(|| chunk_non_null(meta));
        column.compressed_bytes = u64::try_from(meta.compressed_size()).unwrap_or(0);
        column.dictionary = chunk_dictionary(meta);
        column.column_index = col_idx;
        column.fixed_len = fixed_len(&descr);
        column.stats_excess_bytes = stats_excess_bytes(meta);
        columns.push(column);
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

/// A `FIXED_LEN_BYTE_ARRAY` leaf's value width.
fn fixed_len(descr: &ColumnDescriptor) -> Option<i32> {
    match descr.physical_type() {
        parquet::basic::Type::FIXED_LEN_BYTE_ARRAY => Some(descr.type_length()),
        _ => None,
    }
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

/// The bytes a chunk's recorded `min_value` and `max_value` hold beyond the
/// 64-byte length a truncating writer stops at.
///
/// Only variable- and fixed-length statistics can grow: an `INT32`, `INT64`,
/// `FLOAT`, or `DOUBLE` bound is its fixed physical width. A footer without
/// both bounds has nothing to truncate, so it reports none.
fn stats_excess_bytes(meta: &ColumnChunkMetaData) -> u64 {
    let Some(statistics) = meta.statistics() else {
        return 0;
    };
    let bounded = match statistics {
        Statistics::ByteArray(stats) => stats.min_bytes_opt().zip(stats.max_bytes_opt()),
        Statistics::FixedLenByteArray(stats) => stats.min_bytes_opt().zip(stats.max_bytes_opt()),
        _ => None,
    };
    let Some((min, max)) = bounded else {
        return 0;
    };
    excess(min.len()).saturating_add(excess(max.len()))
}

/// The bytes a bound of this length holds beyond the truncation length.
fn excess(len: usize) -> u64 {
    u64::try_from(len)
        .unwrap_or(u64::MAX)
        .saturating_sub(STATISTICS_TRUNCATE_LENGTH)
}

/// The row group whose row count is closest to the file's average, breaking
/// ties by the lowest index.
///
/// One average-sized group prices a codec decision on whole values, while a
/// short leading group or a short trailing tail group would not stand for the
/// rest of the file.
pub(super) fn representative_row_group(metadata: &ParquetMetaData) -> Option<usize> {
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

/// What one column chunk's footer records about its dictionary encoding.
fn chunk_dictionary(meta: &ColumnChunkMetaData) -> ChunkDictionary {
    // A writer that records no dictionary page may still leave the offset field
    // at zero: only a positive offset locates one.
    let dictionary_page_offset = meta.dictionary_page_offset().filter(|offset| *offset > 0);
    let data_page_offset = meta.data_page_offset();
    // A dictionary page's values are PLAIN-encoded, so a chunk's whole encoding
    // set always lists PLAIN beside its dictionary encoding; only the data
    // pages' encodings tell a falling-back chunk from a dictionary-encoded one.
    // A footer that counts no page encodings leaves that to the page headers,
    // which [`pages_fell_back`] reads.
    let fell_back = meta.page_encoding_stats_mask().map(|mask| {
        let plain = mask.is_set(Encoding::PLAIN);
        let dictionary =
            mask.is_set(Encoding::RLE_DICTIONARY) || mask.is_set(Encoding::PLAIN_DICTIONARY);
        plain && dictionary
    });
    ChunkDictionary {
        present: dictionary_page_offset.is_some()
            || records_dictionary_pages(meta)
            || records_dictionary_encoding(meta),
        page_size: dictionary_page_offset
            .filter(|offset| data_page_offset > *offset)
            .and_then(|offset| usize::try_from(data_page_offset - offset).ok()),
        fell_back,
    }
}

/// Whether the footer counts dictionary-encoded data pages: a fact older
/// writers record even when they omit the dictionary page offset.
fn records_dictionary_pages(meta: &ColumnChunkMetaData) -> bool {
    meta.page_encoding_stats().is_some_and(|stats| {
        stats.iter().any(|stat| {
            matches!(stat.page_type, PageType::DATA_PAGE | PageType::DATA_PAGE_V2)
                && matches!(
                    stat.encoding,
                    Encoding::RLE_DICTIONARY | Encoding::PLAIN_DICTIONARY
                )
                && stat.count > 0
        })
    })
}

/// Whether the chunk's encoding set names a dictionary encoding: the other
/// footer fact that marks a dictionary-encoded chunk whose page offset the
/// writer omitted (parquet-mr 1.8.3 does both).
fn records_dictionary_encoding(meta: &ColumnChunkMetaData) -> bool {
    meta.encodings().any(|encoding| {
        matches!(
            encoding,
            Encoding::RLE_DICTIONARY | Encoding::PLAIN_DICTIONARY
        )
    })
}

/// Decode one chunk's cells into the vector of the leaf's physical type.
fn decode_values(
    group: &dyn RowGroupReader,
    col_idx: usize,
    descr: &ColumnDescriptor,
    rows: usize,
) -> Result<DecodedColumn, Error> {
    let max_def = descr.max_def_level();
    let mut column = DecodedColumn::default();
    match group.get_column_reader(col_idx)? {
        ColumnReader::ByteArrayColumnReader(mut reader) => {
            column.bytes = decode_typed(&mut reader, max_def, rows)?;
        }
        ColumnReader::FixedLenByteArrayColumnReader(mut reader) => {
            column.bytes = decode_typed::<FixedLenByteArrayType>(&mut reader, max_def, rows)?
                .into_iter()
                .map(|cell| cell.map(|value| ByteArray::from(value.data().to_vec())))
                .collect();
        }
        ColumnReader::Int32ColumnReader(mut reader) => {
            column.int32_values = decode_typed(&mut reader, max_def, rows)?;
        }
        ColumnReader::Int64ColumnReader(mut reader) => {
            column.int64_values = decode_typed(&mut reader, max_def, rows)?;
        }
        ColumnReader::FloatColumnReader(mut reader) => {
            column.float_values = decode_typed(&mut reader, max_def, rows)?;
        }
        ColumnReader::DoubleColumnReader(mut reader) => {
            column.double_values = decode_typed(&mut reader, max_def, rows)?;
        }
        // A boolean leaf has no rule, and an INT96 leaf's rule reads its own
        // timestamps: neither is decoded here.
        ColumnReader::BoolColumnReader(_) | ColumnReader::Int96ColumnReader(_) => {}
    }
    Ok(column)
}

/// Read `rows` records as one cell each, a null where the definition level is
/// below the leaf's maximum.
fn decode_typed<T: DataType>(
    reader: &mut ColumnReaderImpl<T>,
    max_def: i16,
    rows: usize,
) -> Result<Vec<Option<T::T>>, Error> {
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
            out.extend(values.iter().map(|value| Some(value.clone())));
        } else {
            let mut at = 0;
            for &def in &def_levels[..levels] {
                if def == max_def {
                    out.push(Some(values[at].clone()));
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

/// One page of a column chunk, read from the file's bytes: the payload a codec
/// compresses, the encoding its values use, and, when the file records an
/// offset index, the bytes the page occupies on disk.
pub(crate) struct ChunkPage {
    /// The page's uncompressed payload: exactly the bytes a codec compresses.
    pub(crate) payload: Vec<u8>,
    /// The page's on-disk size, page header included; `None` when the file
    /// records no offset index.
    pub(crate) compressed_page_size: Option<u64>,
    /// The encoding the page's values use. A dictionary page's values are
    /// `PLAIN`; a data page's encoding is what tells a writer's fallback from a
    /// dictionary-encoded chunk.
    pub(crate) encoding: Encoding,
    /// The page holds values: a data page, not the chunk's dictionary page.
    pub(crate) data: bool,
}

/// The sample chunk's pages, read straight from the file: the bytes a codec
/// compresses, without decoding values.
pub(crate) fn chunk_pages(
    path: &Path,
    row_group: usize,
    column_index: usize,
) -> Result<Vec<ChunkPage>, Error> {
    let file = std::fs::File::open(path).map_err(ParquetError::Io)?;
    let reader = SerializedFileReader::new(file)?;
    let recorded_bytes =
        chunk_page_compressed_bytes(path, reader.metadata(), row_group, column_index)?;
    let group = reader.get_row_group(row_group)?;
    let mut pages = group.get_column_page_reader(column_index)?;
    let mut read_pages = Vec::new();
    let mut data_at = 0;
    for page in &mut pages {
        let page = page?;
        let data = page.is_data_page();
        // The offset index lists the chunk's data pages only, in file order.
        let compressed_page_size = if data {
            let bytes = recorded_bytes.get(data_at).copied().flatten();
            data_at += 1;
            bytes
        } else {
            None
        };
        read_pages.push(ChunkPage {
            payload: page.buffer().to_vec(),
            compressed_page_size,
            encoding: page.encoding(),
            data,
        });
    }
    Ok(read_pages)
}

/// Whether a chunk's own data pages fell back to `PLAIN` beside a dictionary,
/// when the footer records no page encoding stats to say so.
///
/// A chunk with a dictionary page whose data pages include both a dictionary
/// encoding and `PLAIN` fell back; a chunk whose data pages use only the
/// dictionary encoding did not. `None` when the chunk has no dictionary page or
/// its pages cannot be read.
pub(super) fn pages_fell_back(
    path: &Path,
    row_group: usize,
    column_index: usize,
    has_dictionary: bool,
) -> Option<bool> {
    if !has_dictionary {
        return Some(false);
    }
    let pages = chunk_pages(path, row_group, column_index).ok()?;
    let mut plain = false;
    let mut dictionary = false;
    for page in pages.iter().filter(|page| page.data) {
        match page.encoding {
            Encoding::PLAIN => plain = true,
            Encoding::RLE_DICTIONARY | Encoding::PLAIN_DICTIONARY => dictionary = true,
            _ => {}
        }
    }
    Some(plain && dictionary)
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
    path: &Path,
    metadata: &ParquetMetaData,
    row_group: usize,
    column_index: usize,
) -> Result<Vec<Option<u64>>, Error> {
    let Some(chunk) = metadata
        .row_groups()
        .get(row_group)
        .and_then(|group| group.columns().get(column_index))
    else {
        return Ok(Vec::new());
    };
    if chunk.offset_index_offset().is_none() {
        return Ok(Vec::new());
    }
    let file = std::fs::File::open(path).map_err(ParquetError::Io)?;
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
        .and_then(|row_groups| row_groups.get(row_group))
        .and_then(|columns| columns.get(column_index))
    else {
        return Ok(Vec::new());
    };
    Ok(index
        .page_locations()
        .iter()
        .map(|location| u64::try_from(location.compressed_page_size).ok())
        .collect())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use parquet::file::metadata::{FileMetaData, ParquetMetaData, RowGroupMetaData};
    use parquet::schema::types::{SchemaDescriptor, Type};

    use super::{excess, representative_row_group};

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

    /// A bound within the truncation length is no bloat, and one beyond it
    /// counts only the bytes past the length.
    #[test]
    fn only_the_bytes_beyond_the_truncation_length_are_excess() {
        assert_eq!(excess(0), 0);
        assert_eq!(excess(64), 0);
        assert_eq!(excess(65), 1);
        assert_eq!(excess(1_024), 960);
    }
}
