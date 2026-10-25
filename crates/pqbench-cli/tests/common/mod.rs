//! Helpers shared by the CLI's blackbox tests.

use std::io::Write as _;
use std::process::{Command, Output, Stdio};

/// The table-file document a local Parquet path names, under `local.local`.
pub(crate) fn table_file(path: &str) -> String {
    let file = path
        .rsplit('/')
        .find(|segment| !segment.is_empty())
        .unwrap_or(path);
    format!(
        "{{\"name\":\"{}\",\"blob\":\"{}\"}}\n",
        pqbench::v1::name::table_file_name(&pqbench::v1::name::local_table(path), file),
        pqbench::v1::name::blob_name(path)
    )
}

/// A `pqbench bytemass` process reading one file's table-file record.
pub(crate) fn measure(path: &str) -> std::process::Child {
    let mut child = Command::new(env!("CARGO_BIN_EXE_pqbench"))
        .arg("bytemass")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(table_file(path).as_bytes())
        .unwrap();
    child
}

/// Measure one file and wait for its records.
pub(crate) fn measured(path: &str) -> Output {
    measure(path).wait_with_output().unwrap()
}
