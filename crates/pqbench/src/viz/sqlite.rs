use std::path::Path;

use rusqlite::Connection;

use super::{FileMass, MassRecord};
use crate::parquet_helpers::Error;

/// Store the collected rows as a SQLite database.
///
/// # Errors
/// Fails when there are no rows or SQLite cannot write `path`.
pub fn write_sqlite(path: &Path, rows: &[MassRecord], files: &[FileMass]) -> Result<(), Error> {
    if rows.is_empty() {
        return Err(Error("no bytemass rows".into()));
    }
    let _ = std::fs::remove_file(path);
    let conn = Connection::open(path).map_err(sqlite_error)?;
    conn.execute_batch(
        "CREATE TABLE masses (
            id TEXT NOT NULL,
            file TEXT NOT NULL,
            size INTEGER NOT NULL,
            num_rows INTEGER NOT NULL,
            column_path TEXT NOT NULL,
            compressed_bytes INTEGER NOT NULL,
            uncompressed_bytes INTEGER NOT NULL,
            codec TEXT NOT NULL
        );
        CREATE TABLE files (
            id TEXT NOT NULL,
            path TEXT NOT NULL,
            file TEXT NOT NULL,
            size INTEGER NOT NULL,
            num_records INTEGER,
            bytes_per_row REAL,
            storage_class TEXT,
            partition TEXT NOT NULL
        );",
    )
    .map_err(sqlite_error)?;
    let mut insert = conn
        .prepare(
            "INSERT INTO masses (
                id, file, size, num_rows, column_path,
                compressed_bytes, uncompressed_bytes, codec
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        )
        .map_err(sqlite_error)?;
    for row in rows {
        insert
            .execute(rusqlite::params![
                row.id,
                row.file,
                row.size,
                row.num_rows,
                row.column,
                row.compressed_bytes,
                row.uncompressed_bytes,
                row.codec,
            ])
            .map_err(sqlite_error)?;
    }
    let mut insert_file = conn
        .prepare(
            "INSERT INTO files (
                id, path, file, size, num_records, bytes_per_row, storage_class, partition
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        )
        .map_err(sqlite_error)?;
    for file in files {
        insert_file
            .execute(rusqlite::params![
                file.id,
                file.path,
                file.file,
                file.size,
                file.num_records,
                file.bytes_per_row,
                file.storage_class,
                file.partition,
            ])
            .map_err(sqlite_error)?;
    }
    Ok(())
}

fn sqlite_error(error: rusqlite::Error) -> Error {
    Error(format!("sqlite: {error}"))
}
