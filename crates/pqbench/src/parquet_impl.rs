//! The bundled `PageParser` implementation, backed by parquet-rs.
//!
//! This module is private (`lib.rs` doesn't `pub mod` it); the only thing the
//! crate exposes is [`crate::parquet_helpers::default_parser`]. Tool code never
//! imports `parquet` directly.

use std::path::Path;

use parquet::basic::Compression;
use parquet::column::page::{Page as ParquetPage, PageReader};
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use parquet::file::reader::{FileReader, SerializedFileReader};

use crate::parquet_helpers::{
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
        let reader = SerializedFileReader::try_from(path)?;
        create_masses(reader.metadata())
    }
}

/// Decode byte masses from a complete Parquet footer without reading pages.
pub(crate) fn read_footer_masses(footer: &[u8]) -> Result<FileMass, Error> {
    let metadata =
        ParquetMetaDataReader::new().parse_and_finish(&bytes::Bytes::copy_from_slice(footer))?;
    create_masses(&metadata)
}

fn create_masses(metadata: &ParquetMetaData) -> Result<FileMass, Error> {
    let mut row_count = 0i64;
    let mut columns = Vec::new();
    for row_group in metadata.row_groups() {
        row_count += row_group.num_rows();
        for meta in row_group.columns() {
            columns.push(ColumnMass {
                column: meta.column_path().string(),
                compressed_bytes: u64::try_from(meta.compressed_size()).unwrap_or(0),
                uncompressed_bytes: u64::try_from(meta.uncompressed_size()).unwrap_or(0),
                codec: meta.compression().to_string(),
            });
        }
    }
    Ok(FileMass {
        row_count: u64::try_from(row_count).unwrap_or(0),
        columns,
    })
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
