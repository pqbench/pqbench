//! `int96-timestamp`: the deprecated INT96 timestamp, priced as a 12-byte
//! `FIXED_LEN_BYTE_ARRAY` against a microsecond `INT64`.

use parquet::basic::Encoding;
use parquet::column::reader::ColumnReader;
use parquet::data_type::{ByteArray, Int96};
use parquet::file::properties::WriterVersion;
use parquet::file::reader::{FileReader, SerializedFileReader};

use crate::third_party::parquet::api::Error as ParquetError;
use crate::v1;

use super::control::{self, Measurement};
use super::price::{price, Settings, Values};
use super::writer::Writer;
use super::{ColumnSample, Error, Finding};

/// The rule id a suggestion carries.
pub const INT96_TIMESTAMP_RULE: &str = "int96-timestamp";

/// An INT96 timestamp's physical width: eight nanoseconds bytes plus the
/// four-byte Julian day.
const INT96_BYTES: i32 = 12;

/// The Julian day of the Unix epoch.
const EPOCH_JULIAN_DAY: i64 = 2_440_588;

/// Microseconds in a day.
const MICROSECONDS_PER_DAY: i64 = 86_400_000_000;

/// One timestamp in both shapes: its 12 physical bytes and its microseconds
/// since the Unix epoch.
type TimestampCells = (Vec<Option<ByteArray>>, Vec<Option<i64>>);

/// The finding this column earns when its leaf is an INT96 timestamp. Nothing
/// is decoded: the footer's physical type is the whole check.
pub(super) fn detect(sample: &ColumnSample<'_>) -> Option<Finding> {
    if sample.column.physical_data_type.as_deref() != Some("INT96") {
        return None;
    }
    Some(Finding {
        message: format!("an INT96 timestamp at {:.2} B/value", sample.text_bpv),
        remedy: remedy(sample.writer_kind()),
        remedies: remedies(),
        measure,
    })
}

/// Decode the sample chunk's INT96 values and price them as microsecond
/// `INT64` against the source's own `FIXED_LEN_BYTE_ARRAY(12)` encoding, at the
/// file's codec and level.
fn measure(sample: &ColumnSample<'_>) -> Result<Option<Measurement>, Error> {
    let Some((int96_cells, micros_cells)) = decode(sample)? else {
        return Ok(None);
    };
    measure_priced(&int96_cells, &micros_cells, sample)
}

/// Price the two physical shapes over the same timestamps.
fn measure_priced(
    int96_cells: &[Option<ByteArray>],
    micros_cells: &[Option<i64>],
    sample: &ColumnSample<'_>,
) -> Result<Option<Measurement>, Error> {
    // The source is a `FIXED_LEN_BYTE_ARRAY(12)` leaf. A dictionary on one
    // exists only under the 2.0 page version, so both arms write v2 pages when
    // the source had a dictionary, and the pair stays like for like.
    let version = if sample.chunk_has_dictionary {
        WriterVersion::PARQUET_2_0
    } else {
        WriterVersion::PARQUET_1_0
    };
    let baseline = Settings {
        dictionary: sample.chunk_has_dictionary,
        dictionary_page_size_limit: sample.dictionary_page_size_limit(),
        data_page_size_limit: None,
        encoding: match (control::source_encoding(sample.mass), version) {
            (None, WriterVersion::PARQUET_2_0) => Some(Encoding::PLAIN),
            (recorded, _) => recorded,
        },
        version,
    };
    control::measure_arms(sample, |compression| {
        let baseline_bytes = price(
            Values::FixedLen(INT96_BYTES, int96_cells),
            compression,
            baseline,
        )?;
        let mut candidate_bytes = u64::MAX;
        for encoding in [Encoding::PLAIN, Encoding::DELTA_BINARY_PACKED] {
            let bytes = price(
                Values::Int64(micros_cells),
                compression,
                Settings {
                    dictionary: false,
                    dictionary_page_size_limit: None,
                    data_page_size_limit: None,
                    encoding: Some(encoding),
                    version: WriterVersion::PARQUET_1_0,
                },
            )?;
            candidate_bytes = candidate_bytes.min(bytes);
        }
        Ok(Some((baseline_bytes, candidate_bytes)))
    })
}

/// Decode the sample chunk into its 12-byte INT96 values and their
/// microseconds since the Unix epoch. `None` when the chunk is not INT96.
fn decode(sample: &ColumnSample<'_>) -> Result<Option<TimestampCells>, Error> {
    let file = std::fs::File::open(sample.path).map_err(ParquetError::Io)?;
    let reader = SerializedFileReader::new(file)?;
    let group = reader.get_row_group(sample.row_group)?;
    let ColumnReader::Int96ColumnReader(mut reader) =
        group.get_column_reader(sample.column_index)?
    else {
        return Ok(None);
    };
    let max_def = sample.column.max_definition_level.unwrap_or(0) as i16;
    let rows = usize::try_from(sample.coverage.row_count).unwrap_or(0);
    let mut int96_cells = Vec::with_capacity(rows);
    let mut micros_cells = Vec::with_capacity(rows);
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
            for value in &values {
                push_cells(&mut int96_cells, &mut micros_cells, value);
            }
        } else {
            let mut at = 0;
            for &def in &def_levels[..levels] {
                if def == max_def {
                    push_cells(&mut int96_cells, &mut micros_cells, &values[at]);
                    at += 1;
                } else {
                    int96_cells.push(None);
                    micros_cells.push(None);
                }
            }
        }
        remaining = remaining.saturating_sub(consumed);
    }
    Ok(Some((int96_cells, micros_cells)))
}

/// Push one decoded timestamp in both shapes.
fn push_cells(
    int96_cells: &mut Vec<Option<ByteArray>>,
    micros_cells: &mut Vec<Option<i64>>,
    value: &Int96,
) {
    int96_cells.push(Some(int96_bytes(value)));
    micros_cells.push(Some(micros(value)));
}

/// A timestamp's 12 physical bytes: the three little-endian words.
fn int96_bytes(value: &Int96) -> ByteArray {
    let mut bytes = [0_u8; INT96_BYTES as usize];
    for (at, word) in value.data().iter().enumerate() {
        bytes[at * 4..at * 4 + 4].copy_from_slice(&word.to_le_bytes());
    }
    ByteArray::from(bytes.to_vec())
}

/// The timestamp as microseconds since the Unix epoch: the Julian day's offset
/// from the epoch times a day's microseconds, plus the nanoseconds' microseconds.
fn micros(value: &Int96) -> i64 {
    let data = value.data();
    let nanos = ((data[1] as i64) << 32) + data[0] as i64;
    (data[2] as i64 - EPOCH_JULIAN_DAY)
        .wrapping_mul(MICROSECONDS_PER_DAY)
        .wrapping_add(nanos / 1_000)
}

/// The settings that write timestamps as microsecond `INT64`, one per engine.
fn remedies() -> Vec<v1::Remedy> {
    vec![
        v1::Remedy::new(
            "spark",
            "spark.sql.parquet.outputTimestampType=TIMESTAMP_MICROS",
        ),
        v1::Remedy::new("pyarrow", "use_deprecated_int96_timestamps=False"),
        v1::Remedy::new(
            "parquet-rs",
            "write the column as INT64 TIMESTAMP(MICROS), not deprecated INT96",
        ),
        v1::Remedy::new(
            "iceberg",
            "ALTER TABLE <table> ALTER COLUMN <column> TYPE timestamp",
        ),
    ]
}

/// The session setting that writes timestamps as microsecond `INT64`, in the
/// detected writer's terms.
fn remedy(writer: Writer) -> String {
    let remedies = remedies();
    let setting = |engine: &str| super::setting(&remedies, engine);
    match writer {
        Writer::ParquetMr => setting("spark"),
        Writer::Photon => format!("{} (Photon forwarding unverified)", setting("spark")),
        Writer::ArrowCpp => setting("pyarrow"),
        Writer::ArrowRs => setting("parquet-rs"),
        Writer::Other => format!(
            "write timestamps as INT64 TIMESTAMP(MICROS), not deprecated INT96 ({})",
            setting("iceberg")
        ),
    }
}

#[cfg(test)]
mod tests {
    use parquet::data_type::Int96;

    use super::{
        detect, int96_bytes, measure_priced, micros, remedy, EPOCH_JULIAN_DAY, MICROSECONDS_PER_DAY,
    };
    use crate::lint::writer::Writer;
    use crate::lint::ColumnSample;
    use crate::v1;

    fn column(physical_data_type: &str) -> v1::Column {
        v1::Column {
            physical_data_type: Some(physical_data_type.into()),
            mass: Some(v1::Bytemass {
                codec: Some("UNCOMPRESSED".into()),
                ..v1::Bytemass::default()
            }),
            ..v1::Column::default()
        }
    }

    fn timestamp(micros_since_epoch: i64) -> Int96 {
        let days = micros_since_epoch.div_euclid(MICROSECONDS_PER_DAY);
        let day_micros = micros_since_epoch.rem_euclid(MICROSECONDS_PER_DAY);
        let nanos = day_micros * 1_000;
        Int96::from(vec![
            nanos as u32,
            (nanos >> 32) as u32,
            (EPOCH_JULIAN_DAY + days) as u32,
        ])
    }

    #[test]
    fn julian_days_convert_to_unix_microseconds() {
        assert_eq!(micros(&timestamp(0)), 0);
        assert_eq!(micros(&timestamp(1)), 1);
        assert_eq!(micros(&timestamp(1_500_000)), 1_500_000);
        // 2001-09-09T01:46:40Z, a round second far from the epoch.
        assert_eq!(
            micros(&timestamp(1_000_000_000_000_000)),
            1_000_000_000_000_000
        );
    }

    #[test]
    fn the_physical_bytes_are_the_three_little_endian_words() {
        let value = Int96::from(vec![0x0403_0201, 0x0807_0605, 0x0000_0c0b]);
        let bytes = [
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x0b, 0x0c, 0x00, 0x00,
        ];
        assert_eq!(int96_bytes(&value).data(), &bytes[..]);
    }

    #[test]
    fn only_int96_is_a_candidate() {
        let cells = Vec::new();
        assert!(detect(&ColumnSample::for_test(&column("INT96"), &cells, false)).is_some());
        for physical in ["INT64", "INT32", "BYTE_ARRAY"] {
            assert!(
                detect(&ColumnSample::for_test(&column(physical), &cells, false)).is_none(),
                "{physical}"
            );
        }
    }

    #[test]
    fn each_writer_gets_a_setting_it_has() {
        assert!(remedy(Writer::ParquetMr).contains("TIMESTAMP_MICROS"));
        assert!(remedy(Writer::Photon).contains("unverified"));
        assert_eq!(
            remedy(Writer::ArrowCpp),
            "use_deprecated_int96_timestamps=False"
        );
        assert!(remedy(Writer::ArrowRs).contains("TIMESTAMP(MICROS)"));
    }

    /// Twelve bytes per value against eight, with Delta binary packing for the
    /// integer arm, is a large saving even before compression.
    #[test]
    fn microseconds_in_an_int64_are_much_smaller_than_int96() {
        let column = column("INT96");
        let sample = ColumnSample::for_test(&column, &[], false);
        let mut int96_cells = Vec::new();
        let mut micros_cells = Vec::new();
        for step in 0..1_000_i64 {
            let value = timestamp(1_000_000_000_000_000 + step * 1_000_000);
            int96_cells.push(Some(int96_bytes(&value)));
            micros_cells.push(Some(micros(&value)));
        }

        let measurement = measure_priced(&int96_cells, &micros_cells, &sample)
            .expect("measure")
            .expect("uncompressed");
        assert!(
            measurement.ratio_low > 0.3,
            "INT64 micros must save at least the INT96 word, got {}",
            measurement.ratio_low
        );
    }
}
