//! `lint::decode`: the raw Parquet mechanism behind a lint sample.
//!
//! This module reads one file's footer and its representative row group; it
//! decodes cells only for a byte leaf, and page-reading rules reread the raw
//! pages from here. It decides nothing about what a column should be: the
//! rules ([`super::RULES`]) and the suggestion policy ([`super::suggestions`],
//! [`super::verify`]) work on the sample this module reports.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use parquet::basic::Encoding;
use parquet::column::reader::{ColumnReader, ColumnReaderImpl};
use parquet::data_type::{ByteArray, DataType};
use parquet::file::metadata::{
    ColumnChunkMetaData, PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader,
};
use parquet::file::reader::{FileReader, RowGroupReader, SerializedFileReader};

use crate::third_party::parquet::api::Error as ParquetError;

use super::Error;

/// What a sample decoded: rows and the whole row groups they came from.
#[derive(Clone, Copy, Default)]
pub(super) struct Coverage {
    pub(super) row_count: u64,
    pub(super) row_group_count: u64,
}

/// What a column chunk's footer records about its dictionary page.
#[derive(Clone, Copy, Default)]
pub(super) struct ChunkDictionary {
    /// The chunk has a dictionary page.
    pub(super) present: bool,
    /// Bytes from the dictionary page offset to the first data page.
    pub(super) page_size: Option<usize>,
    /// The chunk's data pages include PLAIN beside a dictionary encoding, so
    /// its writer fell back to PLAIN.
    pub(super) fell_back: bool,
}

/// One requested column's representative row group chunk.
#[derive(Default)]
pub(super) struct DecodedColumn {
    /// One cell per row of the group; `None` where the column is null. Empty
    /// for a non-byte leaf, whose rules read the footer or the raw pages.
    pub(super) cells: Vec<Option<ByteArray>>,
    /// Non-null values in the chunk, exact when the cells were decoded and
    /// from the footer statistics otherwise.
    pub(super) non_null_value_count: u64,
    /// The chunk's compressed bytes.
    pub(super) compressed_bytes: u64,
    /// The chunk's dictionary page, as its footer records it.
    pub(super) dictionary: ChunkDictionary,
    /// The chunk's leaf index in the file schema.
    pub(super) column_index: usize,
}

/// The representative row group's whole chunks for the requested scalar leaves.
pub(super) struct DecodedLeaves {
    /// Per requested column: its chunk from the representative row group.
    pub(super) columns: Vec<DecodedColumn>,
    pub(super) coverage: Coverage,
    /// The representative row group's index, or zero when the file has none.
    pub(super) row_group: usize,
}

/// Decode the file's one representative row group for the requested scalar
/// leaf columns, directly from their column chunks. Values are decoded only for
/// a byte leaf; any other leaf carries its footer counts, and a page-reading
/// rule rereads its raw pages. Unselected columns are never read.
pub(super) fn decode_leaves(path: &Path, leaves: &[String]) -> Result<DecodedLeaves, Error> {
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
    use std::sync::Arc;

    use parquet::file::metadata::{FileMetaData, ParquetMetaData, RowGroupMetaData};
    use parquet::schema::types::{SchemaDescriptor, Type};

    use super::representative_row_group;

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
