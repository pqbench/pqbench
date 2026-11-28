//! `price`: encode one column's row group in memory and return its compressed
//! bytes.

use std::sync::Arc;

use parquet::basic::{Compression, Encoding, Repetition, Type as PhysicalType};
use parquet::data_type::{
    ByteArray, ByteArrayType, DataType, DoubleType, FixedLenByteArray, FixedLenByteArrayType,
    FloatType, Int32Type, Int64Type,
};
use parquet::file::properties::{WriterProperties, WriterVersion};
use parquet::file::writer::{SerializedColumnWriter, SerializedFileWriter};
use parquet::schema::types::{ColumnPath, Type};

use crate::third_party::parquet::api::Error as ParquetError;

use super::Error;

/// The values one arm writes, in the physical shape it writes them as.
///
/// Every physical type a rule re-encodes has an arm, so a numeric, float, or
/// fixed-length column goes through the same controlled pair as a byte column.
#[derive(Clone, Copy)]
pub(super) enum Values<'a> {
    /// Variable-length `BYTE_ARRAY`: a 4-byte length prefix per value.
    Bytes(&'a [Option<ByteArray>]),
    /// `FIXED_LEN_BYTE_ARRAY` of this many bytes: no length prefix.
    FixedLen(i32, &'a [Option<ByteArray>]),
    /// `INT32` values.
    Int32(&'a [Option<i32>]),
    /// `INT64` values.
    Int64(&'a [Option<i64>]),
    /// `FLOAT` values.
    Float(&'a [Option<f32>]),
    /// `DOUBLE` values.
    Double(&'a [Option<f64>]),
}

impl Values<'_> {
    /// The cells the arm writes, nulls included.
    pub(super) fn len(&self) -> usize {
        match self {
            Self::Bytes(cells) | Self::FixedLen(_, cells) => cells.len(),
            Self::Int32(cells) => cells.len(),
            Self::Int64(cells) => cells.len(),
            Self::Float(cells) => cells.len(),
            Self::Double(cells) => cells.len(),
        }
    }

    /// The arm's schema: one optional column of the arm's physical type.
    fn field(&self) -> Result<Arc<Type>, ParquetError> {
        let (physical, length) = match self {
            Self::Bytes(_) => (PhysicalType::BYTE_ARRAY, None),
            Self::FixedLen(bytes, _) => (PhysicalType::FIXED_LEN_BYTE_ARRAY, Some(*bytes)),
            Self::Int32(_) => (PhysicalType::INT32, None),
            Self::Int64(_) => (PhysicalType::INT64, None),
            Self::Float(_) => (PhysicalType::FLOAT, None),
            Self::Double(_) => (PhysicalType::DOUBLE, None),
        };
        let mut builder =
            Type::primitive_type_builder("value", physical).with_repetition(Repetition::OPTIONAL);
        if let Some(length) = length {
            builder = builder.with_length(length);
        }
        Ok(Arc::new(builder.build()?))
    }

    /// One definition level per cell: 1 for a value, 0 for a null.
    fn definition_levels(&self) -> Vec<i16> {
        let is_null = |present: bool| if present { 1 } else { 0 };
        match self {
            Self::Bytes(cells) | Self::FixedLen(_, cells) => {
                cells.iter().map(|cell| is_null(cell.is_some())).collect()
            }
            Self::Int32(cells) => cells.iter().map(|cell| is_null(cell.is_some())).collect(),
            Self::Int64(cells) => cells.iter().map(|cell| is_null(cell.is_some())).collect(),
            Self::Float(cells) => cells.iter().map(|cell| is_null(cell.is_some())).collect(),
            Self::Double(cells) => cells.iter().map(|cell| is_null(cell.is_some())).collect(),
        }
    }
}

/// One arm's writer settings: everything [`price`] passes to the writer.
#[derive(Clone, Copy)]
pub(super) struct Settings {
    pub(super) dictionary: bool,
    /// `None` keeps parquet-rs's own dictionary page size limit.
    pub(super) dictionary_page_size_limit: Option<usize>,
    /// `None` keeps parquet-rs's own data page size limit.
    pub(super) data_page_size_limit: Option<usize>,
    pub(super) encoding: Option<Encoding>,
    /// The page version the arm writes. A dictionary on a
    /// `FIXED_LEN_BYTE_ARRAY` leaf exists only in the 2.0 page version, so both
    /// arms of such a pair write v2 pages and the comparison stays like for
    /// like.
    pub(super) version: WriterVersion,
}

impl Settings {
    /// A 1.0-version arm: the page version a footer without one records. A
    /// rule that re-encodes a `FIXED_LEN_BYTE_ARRAY` dictionary sets the 2.0
    /// version instead.
    #[cfg(test)]
    pub(super) fn new(dictionary: bool) -> Self {
        Self {
            dictionary,
            dictionary_page_size_limit: None,
            data_page_size_limit: None,
            encoding: None,
            version: WriterVersion::PARQUET_1_0,
        }
    }
}

/// Encode `values` as one row group of a single optional column and return its
/// compressed bytes.
pub(super) fn price(
    values: Values<'_>,
    compression: Compression,
    settings: Settings,
) -> Result<u64, Error> {
    if values.len() == 0 {
        return Ok(0);
    }
    let levels = values.definition_levels();
    let schema = Arc::new(
        Type::group_type_builder("schema")
            .with_fields(vec![values.field()?])
            .build()?,
    );
    let mut builder = WriterProperties::builder()
        .set_compression(compression)
        .set_writer_version(settings.version)
        .set_dictionary_enabled(settings.dictionary);
    if let Some(limit) = settings.dictionary_page_size_limit {
        builder = builder.set_dictionary_page_size_limit(limit);
    }
    if let Some(limit) = settings.data_page_size_limit {
        builder = builder.set_data_page_size_limit(limit);
    }
    if let Some(encoding) = settings.encoding {
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
        Values::Bytes(cells) => {
            let present: Vec<ByteArray> = cells.iter().flatten().cloned().collect();
            write_batch::<ByteArrayType>(&mut column, &present, &levels)?;
        }
        Values::FixedLen(_, cells) => {
            let present: Vec<FixedLenByteArray> = cells
                .iter()
                .flatten()
                .map(|bytes| FixedLenByteArray::from(bytes.clone()))
                .collect();
            write_batch::<FixedLenByteArrayType>(&mut column, &present, &levels)?;
        }
        Values::Int32(cells) => {
            let present: Vec<i32> = cells.iter().flatten().copied().collect();
            write_batch::<Int32Type>(&mut column, &present, &levels)?;
        }
        Values::Int64(cells) => {
            let present: Vec<i64> = cells.iter().flatten().copied().collect();
            write_batch::<Int64Type>(&mut column, &present, &levels)?;
        }
        Values::Float(cells) => {
            let present: Vec<f32> = cells.iter().flatten().copied().collect();
            write_batch::<FloatType>(&mut column, &present, &levels)?;
        }
        Values::Double(cells) => {
            let present: Vec<f64> = cells.iter().flatten().copied().collect();
            write_batch::<DoubleType>(&mut column, &present, &levels)?;
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
    use parquet::basic::{Compression, Encoding};
    use parquet::data_type::ByteArray;
    use parquet::file::properties::WriterVersion;

    use super::{price, Settings, Values};

    fn cells(values: usize, bytes: usize) -> Vec<Option<ByteArray>> {
        (0..values)
            .map(|value| Some(ByteArray::from(vec![value as u8; bytes])))
            .collect()
    }

    #[test]
    fn a_fixed_length_column_pays_no_length_prefix() {
        let cells = cells(1_000, 16);
        let uncompressed = Compression::UNCOMPRESSED;
        let variable = price(Values::Bytes(&cells), uncompressed, Settings::new(false))
            .expect("price BYTE_ARRAY");
        let fixed = price(
            Values::FixedLen(16, &cells),
            uncompressed,
            Settings::new(false),
        )
        .expect("price FIXED_LEN_BYTE_ARRAY");

        assert!(fixed < variable, "{fixed} !< {variable}");
        assert_eq!(
            (variable - fixed) / cells.len() as u64,
            4,
            "a 4-byte length prefix per value: {variable} vs {fixed}"
        );
    }

    /// A dense integer column encoded as Delta binary packing costs far less
    /// than PLAIN's eight bytes per value.
    #[test]
    fn delta_binary_packed_beats_plain_integers() {
        let cells: Vec<Option<i64>> = (0..10_000).map(|value| Some(i64::from(value))).collect();
        let uncompressed = Compression::UNCOMPRESSED;
        let plain =
            price(Values::Int64(&cells), uncompressed, Settings::new(false)).expect("price PLAIN");
        let delta = price(
            Values::Int64(&cells),
            uncompressed,
            Settings {
                encoding: Some(Encoding::DELTA_BINARY_PACKED),
                ..Settings::new(false)
            },
        )
        .expect("price DELTA_BINARY_PACKED");
        assert!(
            delta * 4 < plain,
            "packed deltas must be much smaller than 8 bytes a value: {delta} vs {plain}"
        );
    }

    /// A float column encodes to about four bytes a value, and a double to
    /// about eight.
    #[test]
    fn float_and_double_arms_write_their_widths() {
        let floats: Vec<Option<f32>> = (0..1_000).map(|value| Some(value as f32)).collect();
        let doubles: Vec<Option<f64>> = (0..1_000).map(|value| Some(value as f64)).collect();
        let uncompressed = Compression::UNCOMPRESSED;
        let float =
            price(Values::Float(&floats), uncompressed, Settings::new(false)).expect("price FLOAT");
        let double = price(Values::Double(&doubles), uncompressed, Settings::new(false))
            .expect("price DOUBLE");
        assert!(double > float, "{double} !> {float}");
        assert!(
            double - float >= 3_000,
            "eight bytes a value against four: {double} vs {float}"
        );
    }

    /// A `FIXED_LEN_BYTE_ARRAY(16)` dictionary needs the 2.0 page version;
    /// the pair's settings carry it so both arms write the same page layout.
    #[test]
    fn a_fixed_length_dictionary_writes_under_the_2_0_version() {
        let cells: Vec<Option<ByteArray>> = (0..2_000)
            .map(|value| Some(ByteArray::from(vec![value as u8; 16])))
            .collect();
        let uncompressed = Compression::UNCOMPRESSED;
        let plain = price(
            Values::FixedLen(16, &cells),
            uncompressed,
            Settings {
                version: WriterVersion::PARQUET_2_0,
                encoding: Some(Encoding::PLAIN),
                ..Settings::new(false)
            },
        )
        .expect("price PLAIN");
        let dictionary = price(
            Values::FixedLen(16, &cells),
            uncompressed,
            Settings {
                version: WriterVersion::PARQUET_2_0,
                ..Settings::new(true)
            },
        )
        .expect("price a dictionary");
        assert!(
            dictionary * 4 < plain,
            "16 distinct values in 2,000 rows must dictionary-encode: {dictionary} vs {plain}"
        );
    }
}
