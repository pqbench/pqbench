//! `price`: encode a rewrite's values and measure the bytes.

use crate::third_party::parquet::api::{
    read_buffer_masses, write_parquet, Kind, TypedColumn, Value, WriteOptions,
};

use super::Error;

/// Encode one byte column of `rows` in memory with these writer settings and
/// return its compressed bytes.
pub(super) fn price_bytes(rows: &[Vec<Value>], options: &WriteOptions) -> Result<u64, Error> {
    let encoded = write_parquet(
        &[TypedColumn {
            name: "value".into(),
            kind: Kind::Bytes,
        }],
        rows,
        options,
    )?;
    let mass = read_buffer_masses(&encoded)?;
    Ok(mass
        .columns
        .first()
        .map_or(0, |column| column.compressed_bytes))
}
