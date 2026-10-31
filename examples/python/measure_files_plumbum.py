#!/usr/bin/env python3
"""The measure_files.py walk, written with plumbum's pipe operator.

    pip install plumbum
    python3 examples/python/measure_files_plumbum.py examples/quickstart.parquet

Compare this with measure_files.py: plumbum builds the same two-process
pipeline, connects the pipes, and waits for both stages. The records still
arrive one at a time.
"""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Iterator

from plumbum import local


def stream(pqbench: str, paths: list[str], fan_out: int) -> Iterator[dict]:
    """Yield the Blob and Bytemass records of `paths` as they arrive."""
    pqb = local[pqbench]
    pipeline = pqb["table-file", *paths] | pqb["bytemass", "--fan-out", str(fan_out)]
    process = pipeline.popen()
    if process.stdout is None:
        raise SystemExit("plumbum did not provide a stdout pipe")
    try:
        for line in process.stdout:
            yield json.loads(line)
    finally:
        process.stdout.close()
        # `wait` returns a nonzero status if any stage failed.
        status = process.wait()
        if status != 0:
            raise SystemExit(f"{pqbench} pipeline exited {status}")


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
