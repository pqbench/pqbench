//! Price a rewrite: encode a sample with writer settings and measure it.

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

/// The writer settings used to price a rewrite: the file's codec when the
/// writer has it (zstd otherwise), no dictionary, and one row group.
pub(super) fn write_options(codec: Option<&str>) -> WriteOptions {
    let rendered = codec.unwrap_or("zstd").to_lowercase();
    let codec = match rendered.split('(').next().unwrap_or("zstd").trim() {
        "uncompressed" | "none" => "uncompressed",
        "snappy" => "snappy",
        "gzip" => "gzip",
        "lz4" | "lz4_raw" => "lz4",
        "zstd" => "zstd",
        _ => "zstd",
    };
    WriteOptions {
        compression: codec.to_string(),
        level: (codec == "zstd").then_some(3),
        dictionary: false,
        dictionary_bytes: None,
        encoding: None,
        page_size: None,
        sorting_columns: Vec::new(),
        row_group_size: 0,
    }
}
