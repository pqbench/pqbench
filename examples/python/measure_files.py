#!/usr/bin/env python3
"""Measure Parquet files with pqbench, streaming the records through Python.

    python3 examples/python/measure_files.py examples/quickstart.parquet

`pqbench table-file` names the files and `pqbench bytemass` reads their
footers. The two processes are connected by an OS pipe: the first record is
parsed while the second file is still being measured, and no intermediate file
or full record list is kept.

Prints a CSV header and one row: files, rows, column chunks, file bytes, and
column bytes.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from collections.abc import Iterator


def stream(pqbench: str, paths: list[str], fan_out: int) -> Iterator[dict]:
    """Yield the Blob and Bytemass records of `paths` as they arrive."""
    names = subprocess.Popen(
        [pqbench, "table-file", *paths],
        stdout=subprocess.PIPE,
        text=True,
    )
    mass = subprocess.Popen(
        [pqbench, "bytemass", "--fan-out", str(fan_out)],
        stdin=names.stdout,
        stdout=subprocess.PIPE,
        text=True,
    )
    # The parent must drop its copy of the producer's stdout, or the consumer
    # never sees EOF after the producer exits.
    if names.stdout is None:
        raise SystemExit("pqbench table-file did not provide a stdout pipe")
    names.stdout.close()
    if mass.stdout is None:
        raise SystemExit("pqbench bytemass did not provide a stdout pipe")
    try:
        for line in mass.stdout:
            yield json.loads(line)
    finally:
        mass.stdout.close()
        # A downstream stage can exit 0 after an earlier stage failed, so check
        # both return codes.
        for process in (names, mass):
            status = process.wait()
            if status != 0:
                raise SystemExit(f"{' '.join(process.args[:2])} exited {status}")


def summarize(records: Iterator[dict]) -> tuple[int, int, int, int, int]:
    """Fold the stream into (files, rows, chunks, file bytes, column bytes)."""
    files = 0
    rows = 0
    column_chunks = 0
    file_bytes = 0
    column_bytes = 0
    first_column: dict[str, str] = {}

    for record in records:
        if "row_group_count" in record:  # Blob: one per file
            files += 1
            file_bytes += record.get("size_bytes", 0)
        elif "compressed_bytes" in record:  # Bytemass: one per column
            column_chunks += 1
            column_bytes += record.get("compressed_bytes", 0)
            blob, _, column = record.get("name", "").rpartition("/columns/")
            column = column.removesuffix("/bytemass")
            # A row group's value_count + null_count is the file's row count;
            # count it once per file, from the first column.
            if first_column.setdefault(blob, column) == column:
                rows += record.get("value_count", 0) + record.get("null_count", 0)

    return files, rows, column_chunks, file_bytes, column_bytes


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("paths", nargs="+", help="Parquet paths or storage URIs")
    parser.add_argument(
        "--fan-out",
        type=int,
        default=128,
        help="files in flight (default: 128)",
    )
    parser.add_argument(
        "--pqbench",
        default=os.environ.get("PQB", "pqbench"),
        help="pqbench binary (default: $PQB, then pqbench)",
    )
    args = parser.parse_args()

    files, rows, chunks, file_bytes, column_bytes = summarize(
        stream(args.pqbench, args.paths, args.fan_out)
    )
    print("files,rows,column_chunks,file_bytes,column_bytes")
    print(f"{files},{rows},{chunks},{file_bytes},{column_bytes}")


if __name__ == "__main__":
    main()
