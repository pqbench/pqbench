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
    let field = Arc::new(
        Type::primitive_type_builder("value", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::OPTIONAL)
            .build()?,
    );
    let schema = Arc::new(
        Type::group_type_builder("schema")
            .with_fields(vec![field])
            .build()?,
    );

    let mut builder = WriterProperties::builder()
        .set_compression(compression)
        .set_dictionary_enabled(dictionary);
    if let Some(encoding) = encoding {
        builder = builder.set_column_encoding(ColumnPath::from("value"), encoding);
    }
    let properties = Arc::new(builder.build());

    let mut out = Vec::new();
    let mut writer = SerializedFileWriter::new(&mut out, schema, properties)?;
    for cells in groups {
        if cells.is_empty() {
            continue;
        }
        let mut group = writer.next_row_group()?;
        let mut column = group
            .next_column()?
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
            .write_batch(&values, Some(&def), None)?;
        column.close()?;
        group.close()?;
    }
    let meta = writer.close()?;
    let bytes = meta
        .row_groups()
        .iter()
        .map(|group| u64::try_from(group.column(0).compressed_size()).unwrap_or(0))
        .sum();
    Ok(bytes)
}
