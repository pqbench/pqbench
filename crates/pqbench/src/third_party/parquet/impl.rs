//! The bundled `PageParser` implementation, backed by parquet-rs.
//!
//! This module is private (`lib.rs` doesn't `pub mod` it); the only thing the
//! crate exposes is [`super::api::default_parser`]. Tool code never
//! imports `parquet` directly.

use std::path::Path;

use parquet::basic::Compression;
use parquet::column::page::{Page as ParquetPage, PageReader};
use parquet::data_type::AsBytes;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::statistics::Statistics;

use super::api::{
    ColumnChunk, ColumnMass, Error, FileMass, MetadataParser, Page, PageParser, ParquetFile,
};

/// The parquet-rs-backed page parser.
pub struct ParquetRsParser;

impl PageParser for ParquetRsParser {
    /// Read a parquet file from an in-memory byte slice and extract its pages.
    ///
    /// No disk I/O: `bytes` is wrapped as an in-memory `ChunkReader` and every
    /// page payload is copied out into the returned [`ParquetFile`].
    fn parse_pages(&self, bytes: &[u8]) -> Result<ParquetFile, Error> {
        let reader = SerializedFileReader::new(bytes::Bytes::copy_from_slice(bytes))?;
        let mut file = ParquetFile { chunks: Vec::new() };
        for rg in 0..reader.num_row_groups() {
            let row_group = reader.get_row_group(rg)?;
            for col in 0..row_group.num_columns() {
                let meta = row_group.metadata().column(col);
                let column = meta.column_path().string();
                let compression = meta.compression();
                if compression != Compression::UNCOMPRESSED {
                    return Err(Error(format!(
                        "only NONE (uncompressed) parquet is supported; column {column} is {compression}"
                    )));
                }
                let pages = collect_pages(row_group.get_column_page_reader(col)?)?;
                file.chunks.push(ColumnChunk { column, pages });
            }
        }
        Ok(file)
    }
}

impl MetadataParser for ParquetRsParser {
    /// Read a parquet file's per-column byte masses from its footer metadata.
    ///
    /// Only the footer is read: no page is decoded, so compressed columns are
    /// fine, and large files aren't loaded into memory.
    fn read_masses(&self, path: &Path) -> Result<FileMass, Error> {
        read_file_masses(path, false)
    }
}

/// Decode byte masses from a complete Parquet footer without reading pages.
pub(crate) fn read_footer_masses(footer: &[u8]) -> Result<FileMass, Error> {
    let metadata =
        ParquetMetaDataReader::new().parse_and_finish(&bytes::Bytes::copy_from_slice(footer))?;
    create_masses(&metadata)
}

/// Lowest ColumnIndex/OffsetIndex offset in a footer, when one exists.
pub(crate) fn page_index_start(footer: &[u8]) -> Result<Option<u64>, Error> {
    let metadata =
        ParquetMetaDataReader::new().parse_and_finish(&bytes::Bytes::copy_from_slice(footer))?;
    Ok(index_start(&metadata))
}

/// Decode footer + page indexes from a file suffix (`tail` ends at `file_size`).
pub(crate) fn read_tail_masses(tail: &[u8], file_size: u64) -> Result<FileMass, Error> {
    let mut reader = ParquetMetaDataReader::new().with_page_index_policy(PageIndexPolicy::Optional);
    reader.try_parse_sized(&bytes::Bytes::copy_from_slice(tail), file_size)?;
    create_masses(&reader.finish()?)
}

fn index_start(metadata: &ParquetMetaData) -> Option<u64> {
    metadata
        .row_groups()
        .iter()
        .flat_map(|row_group| row_group.columns())
        .filter_map(|column| {
            let column_index = column
                .column_index_offset()
                .and_then(|offset| u64::try_from(offset).ok());
            let offset_index = column
                .offset_index_offset()
                .and_then(|offset| u64::try_from(offset).ok());
            match (column_index, offset_index) {
                (Some(left), Some(right)) => Some(left.min(right)),
                (Some(offset), None) | (None, Some(offset)) => Some(offset),
                (None, None) => None,
            }
        })
        .min()
}

/// Read byte masses from a local file. `indexes` loads ColumnIndex/OffsetIndex.
pub(crate) fn read_file_masses(path: &Path, indexes: bool) -> Result<FileMass, Error> {
    let file = std::fs::File::open(path).map_err(|error| Error(error.to_string()))?;
    let policy = if indexes {
        PageIndexPolicy::Optional
    } else {
        PageIndexPolicy::Skip
    };
    let metadata = ParquetMetaDataReader::new()
        .with_page_index_policy(policy)
        .parse_and_finish(&file)?;
    create_masses(&metadata)
}

fn create_masses(metadata: &ParquetMetaData) -> Result<FileMass, Error> {
    let mut row_count = 0i64;
    let mut columns = Vec::new();
    for (row_group_index, row_group) in metadata.row_groups().iter().enumerate() {
        let row_group_rows = u64::try_from(row_group.num_rows()).unwrap_or(0);
        row_count += row_group.num_rows();
        for (column_index, meta) in row_group.columns().iter().enumerate() {
            let stats = meta.statistics();
            let pages = metadata
                .offset_index()
                .and_then(|indexes| indexes.get(row_group_index))
                .and_then(|row| row.get(column_index));
            columns.push(ColumnMass {
                column: meta.column_path().string(),
                compressed_bytes: u64::try_from(meta.compressed_size()).unwrap_or(0),
                uncompressed_bytes: u64::try_from(meta.uncompressed_size()).unwrap_or(0),
                codec: meta.compression().to_string(),
                encodings: meta
                    .encodings()
                    .map(|encoding| encoding.to_string())
                    .collect(),
                num_values: u64::try_from(meta.num_values()).unwrap_or(0),
                dictionary: meta.dictionary_page_offset().is_some(),
                null_count: stats.and_then(Statistics::null_count_opt),
                distinct_count: stats.and_then(Statistics::distinct_count_opt),
                min_value: stats.and_then(|value| stat_text(value, true)),
                max_value: stats.and_then(|value| stat_text(value, false)),
                physical_type: meta.column_type().to_string(),
                row_group: u32::try_from(row_group_index).unwrap_or(0),
                row_group_rows,
                page_count: pages.map(|index| index.page_locations().len() as u64),
                page_compressed_bytes: pages.map(|index| {
                    index
                        .page_locations()
                        .iter()
                        .map(|page| u64::try_from(page.compressed_page_size).unwrap_or(0))
                        .sum()
                }),
            });
        }
    }
    Ok(FileMass {
        row_count: u64::try_from(row_count).unwrap_or(0),
        row_group_count: metadata.row_groups().len(),
        sorting_columns: sorting_columns(metadata),
        columns,
    })
}

fn sorting_columns(metadata: &ParquetMetaData) -> Vec<String> {
    let Some(row_group) = metadata.row_groups().first() else {
        return Vec::new();
    };
    let Some(columns) = row_group.sorting_columns() else {
        return Vec::new();
    };
    columns
        .iter()
        .filter_map(|column| {
            let path = row_group
                .columns()
                .get(usize::try_from(column.column_idx).ok()?)?
                .column_path()
                .string();
            Some(if column.descending {
                format!("{path} DESC")
            } else {
                path
            })
        })
        .collect()
}

fn stat_text(stats: &Statistics, min: bool) -> Option<String> {
    match stats {
        Statistics::Boolean(value) => pick(value, min).map(ToString::to_string),
        Statistics::Int32(value) => pick(value, min).map(ToString::to_string),
        Statistics::Int64(value) => pick(value, min).map(ToString::to_string),
        Statistics::Float(value) => pick(value, min).map(ToString::to_string),
        Statistics::Double(value) => pick(value, min).map(ToString::to_string),
        Statistics::ByteArray(value) => {
            pick(value, min).and_then(|value| bytes_text(value.as_bytes()))
        }
        Statistics::FixedLenByteArray(value) => {
            pick(value, min).and_then(|value| bytes_text(value.as_bytes()))
        }
        Statistics::Int96(_) => None,
    }
}

fn pick<T>(stats: &parquet::file::statistics::ValueStatistics<T>, min: bool) -> Option<&T> {
    if min {
        stats.min_opt()
    } else {
        stats.max_opt()
    }
}

fn bytes_text(value: &[u8]) -> Option<String> {
    let text = std::str::from_utf8(value).ok()?;
    text.chars()
        .all(|ch| !ch.is_control())
        .then(|| text.to_string())
}

fn collect_pages(reader: Box<dyn PageReader>) -> Result<Vec<Page>, Error> {
    let mut out = Vec::new();
    for page in reader {
        let page = page?;
        out.push(create_page(page));
    }
    Ok(out)
}

fn create_page(page: ParquetPage) -> Page {
    Page {
        payload: page.buffer().to_vec(),
        value_count: page.num_values(),
        dictionary: page.is_dictionary_page(),
    }
}

impl From<parquet::errors::ParquetError> for Error {
    fn from(e: parquet::errors::ParquetError) -> Self {
        Error(e.to_string())
    }
}
