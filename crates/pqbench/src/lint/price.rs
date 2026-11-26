//! `price`: encode one byte column's row groups in memory and return its
//! compressed bytes.

use std::sync::Arc;

use parquet::basic::{Compression, Encoding, Repetition, Type as PhysicalType};
use parquet::data_type::{ByteArray, ByteArrayType};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::types::{ColumnPath, Type};

use crate::third_party::parquet::api::Error as ParquetError;

use super::Error;

/// Encode `groups` (one slice per row group, preserving exact row-group
/// boundaries) as a single optional BYTE_ARRAY column and return the total
/// compressed bytes across all written column chunks.
pub(super) fn price_bytes(
    groups: &[Vec<Option<ByteArray>>],
    compression: Compression,
    dictionary: bool,
    encoding: Option<Encoding>,
) -> Result<u64, Error> {
    let field = Type::primitive_type_builder("value", PhysicalType::BYTE_ARRAY)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .map(Arc::new)
        .map_err(|error| ParquetError::Parquet(Box::new(error)))?;
    let schema = Type::group_type_builder("schema")
        .with_fields(vec![field])
        .build()
        .map(Arc::new)
        .map_err(|error| ParquetError::Parquet(Box::new(error)))?;

    let mut builder = WriterProperties::builder()
        .set_compression(compression)
        .set_dictionary_enabled(dictionary);
    if let Some(encoding) = encoding {
        builder = builder.set_column_encoding(ColumnPath::from("value"), encoding);
    }
    let properties = Arc::new(builder.build());

    let mut out = Vec::new();
    let mut writer = SerializedFileWriter::new(&mut out, schema, properties)
        .map_err(|error| ParquetError::Parquet(Box::new(error)))?;
    for cells in groups {
        if cells.is_empty() {
            continue;
        }
        let mut group = writer
            .next_row_group()
            .map_err(|error| ParquetError::Parquet(Box::new(error)))?;
        let mut column = group
            .next_column()
            .map_err(|error| ParquetError::Parquet(Box::new(error)))?
            .ok_or_else(|| ParquetError::Invalid("writer ran out of columns".into()))?;
        let mut values = Vec::with_capacity(cells.len());
        let mut def = Vec::with_capacity(cells.len());
        for cell in cells {
            match cell {
                Some(bytes) => {
                    values.push(bytes.clone());
                    def.push(1_i16);
                }
                None => def.push(0_i16),
            }
        }
        column
            .typed::<ByteArrayType>()
            .write_batch(&values, Some(&def), None)
            .map_err(|error| ParquetError::Parquet(Box::new(error)))?;
        column
            .close()
            .map_err(|error| ParquetError::Parquet(Box::new(error)))?;
        group
            .close()
            .map_err(|error| ParquetError::Parquet(Box::new(error)))?;
    }
    let meta = writer
        .close()
        .map_err(|error| ParquetError::Parquet(Box::new(error)))?;
    let bytes = meta
        .row_groups()
        .iter()
        .map(|group| u64::try_from(group.column(0).compressed_size()).unwrap_or(0))
        .sum();
    Ok(bytes)
}
