//! `price`: encode one column's row group in memory and return its compressed
//! bytes.

use std::sync::Arc;

use parquet::basic::{Compression, Encoding, Repetition, Type as PhysicalType};
use parquet::data_type::{
    ByteArray, ByteArrayType, DataType, FixedLenByteArray, FixedLenByteArrayType, Int64Type,
};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::{SerializedColumnWriter, SerializedFileWriter};
use parquet::schema::types::{ColumnPath, Type};

use crate::third_party::parquet::api::Error as ParquetError;

use super::Error;

/// The physical type an arm's column is written as.
#[derive(Clone, Copy)]
pub(super) enum PhysicalShape {
    /// Variable-length `BYTE_ARRAY`: a 4-byte length prefix per value.
    ByteArray,
    /// `FIXED_LEN_BYTE_ARRAY` of this many bytes: no length prefix.
    FixedLen(i32),
}

/// The values one arm writes, in the physical shape it writes them as.
#[derive(Clone, Copy)]
enum Values<'a> {
    Bytes(PhysicalShape, &'a [Option<ByteArray>]),
    Int64(&'a [Option<i64>]),
}

/// One arm's writer settings: everything [`price_bytes`] passes to the writer.
#[derive(Clone, Copy)]
pub(super) struct Settings {
    pub(super) shape: PhysicalShape,
    pub(super) dictionary: bool,
    /// `None` keeps parquet-rs's own dictionary page size limit.
    pub(super) dictionary_page_size_limit: Option<usize>,
    pub(super) encoding: Option<Encoding>,
}

/// One `INT64` arm's writer settings.
#[derive(Clone, Copy)]
pub(super) struct Int64Settings {
    pub(super) dictionary: bool,
    pub(super) encoding: Option<Encoding>,
}

/// Encode `cells` as one row group of a single optional byte column and return
/// its compressed bytes.
pub(super) fn price_bytes(
    cells: &[Option<ByteArray>],
    compression: Compression,
    settings: Settings,
) -> Result<u64, Error> {
    if cells.is_empty() {
        return Ok(0);
    }
    let schema = Arc::new(
        Type::group_type_builder("schema")
            .with_fields(vec![field(settings.shape)?])
            .build()?,
    );
    price(
        schema,
        compression,
        settings.dictionary,
        settings.dictionary_page_size_limit,
        settings.encoding,
        Values::Bytes(settings.shape, cells),
    )
}

/// Encode `cells` as one row group of a single optional `INT64` column and
/// return its compressed bytes.
pub(super) fn price_int64(
    cells: &[Option<i64>],
    compression: Compression,
    settings: Int64Settings,
) -> Result<u64, Error> {
    if cells.is_empty() {
        return Ok(0);
    }
    let schema = Arc::new(
        Type::group_type_builder("schema")
            .with_fields(vec![int64_field()?])
            .build()?,
    );
    price(
        schema,
        compression,
        settings.dictionary,
        None,
        settings.encoding,
        Values::Int64(cells),
    )
}

/// Write one column of one row group at `compression` and return its
/// compressed bytes.
fn price(
    schema: Arc<Type>,
    compression: Compression,
    dictionary: bool,
    dictionary_page_size_limit: Option<usize>,
    encoding: Option<Encoding>,
    values: Values<'_>,
) -> Result<u64, Error> {
    let levels: Vec<i16> = match values {
        Values::Bytes(_, cells) => cells
            .iter()
            .map(|cell| if cell.is_some() { 1 } else { 0 })
            .collect(),
        Values::Int64(cells) => cells
            .iter()
            .map(|cell| if cell.is_some() { 1 } else { 0 })
            .collect(),
    };
    let mut builder = WriterProperties::builder()
        .set_compression(compression)
        .set_dictionary_enabled(dictionary);
    if let Some(limit) = dictionary_page_size_limit {
        builder = builder.set_dictionary_page_size_limit(limit);
    }
    if let Some(encoding) = encoding {
        builder = builder.set_column_encoding(ColumnPath::from("value"), encoding);
    }
    let properties = Arc::new(builder.build());

    let mut out = Vec::new();
    let mut writer = SerializedFileWriter::new(&mut out, schema, properties)?;
    let mut group = writer.next_row_group()?;
    let mut column = group
        .next_column()?
        .ok_or_else(|| ParquetError::Invalid("writer ran out of columns".into()))?;
    match values {
        Values::Bytes(PhysicalShape::ByteArray, cells) => {
            let values: Vec<ByteArray> = cells.iter().flatten().cloned().collect();
            write_batch::<ByteArrayType>(&mut column, &values, &levels)?;
        }
        Values::Bytes(PhysicalShape::FixedLen(_), cells) => {
            let values: Vec<FixedLenByteArray> = cells
                .iter()
                .flatten()
                .map(|bytes| FixedLenByteArray::from(bytes.clone()))
                .collect();
            write_batch::<FixedLenByteArrayType>(&mut column, &values, &levels)?;
        }
        Values::Int64(cells) => {
            let values: Vec<i64> = cells.iter().flatten().copied().collect();
            write_batch::<Int64Type>(&mut column, &values, &levels)?;
        }
    }
    column.close()?;
    group.close()?;
    let meta = writer.close()?;
    let bytes = meta
        .row_groups()
        .iter()
        .map(|group| u64::try_from(group.column(0).compressed_size()).unwrap_or(0))
        .sum();
    Ok(bytes)
}

/// The optional value column, as the shape's physical type spells it.
fn field(shape: PhysicalShape) -> Result<Arc<Type>, ParquetError> {
    let (physical, length) = match shape {
        PhysicalShape::ByteArray => (PhysicalType::BYTE_ARRAY, None),
        PhysicalShape::FixedLen(bytes) => (PhysicalType::FIXED_LEN_BYTE_ARRAY, Some(bytes)),
    };
    let mut builder =
        Type::primitive_type_builder("value", physical).with_repetition(Repetition::OPTIONAL);
    if let Some(length) = length {
        builder = builder.with_length(length);
    }
    Ok(Arc::new(builder.build()?))
}

/// The optional `INT64` value column.
fn int64_field() -> Result<Arc<Type>, ParquetError> {
    Ok(Arc::new(
        Type::primitive_type_builder("value", PhysicalType::INT64)
            .with_repetition(Repetition::OPTIONAL)
            .build()?,
    ))
}

/// Write one batch of a column's values beside their definition levels.
fn write_batch<T: DataType>(
    column: &mut SerializedColumnWriter<'_>,
    values: &[T::T],
    levels: &[i16],
) -> Result<(), ParquetError> {
    column
        .typed::<T>()
        .write_batch(values, Some(levels), None)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use parquet::basic::Compression;
    use parquet::data_type::ByteArray;

    use super::{price_bytes, PhysicalShape, Settings};

    fn cells(values: usize, bytes: usize) -> Vec<Option<ByteArray>> {
        (0..values)
            .map(|value| Some(ByteArray::from(vec![value as u8; bytes])))
            .collect()
    }

    fn settings(shape: PhysicalShape) -> Settings {
        Settings {
            shape,
            dictionary: false,
            dictionary_page_size_limit: None,
            encoding: None,
        }
    }

    #[test]
    fn a_fixed_length_column_pays_no_length_prefix() {
        let cells = cells(1_000, 16);
        let variable = price_bytes(
            &cells,
            Compression::UNCOMPRESSED,
            settings(PhysicalShape::ByteArray),
        )
        .expect("price BYTE_ARRAY");
        let fixed = price_bytes(
            &cells,
            Compression::UNCOMPRESSED,
            settings(PhysicalShape::FixedLen(16)),
        )
        .expect("price FIXED_LEN_BYTE_ARRAY");

        assert!(fixed < variable, "{fixed} !< {variable}");
        assert_eq!(
            (variable - fixed) / cells.len() as u64,
            4,
            "a 4-byte length prefix per value: {variable} vs {fixed}"
        );
    }
}
