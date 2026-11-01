# Python examples

Two scripts that measure Parquet files through `pqbench` and parse the records
as they arrive. They are the runnable examples behind the
[Python orchestration guide](../../docs/guides/python.md).

- [`measure_files.py`](measure_files.py) uses only the standard library. It
  starts `pqbench table-file` and `pqbench bytemass` with `Popen`, connects
  them with an OS pipe, and folds each record into a CSV row.
- [`measure_files_plumbum.py`](measure_files_plumbum.py) is the same walk with
  [plumbum](https://plumbum.readthedocs.io/)'s `|` operator. Compare the two to
  see what the library buys you.

## Run

```console run
$ python3 examples/python/measure_files.py examples/quickstart.parquet
files,rows,column_chunks,file_bytes,column_bytes
1,8,2,541,170
```

`file_bytes` is the size of the Parquet files; `column_bytes` is the sum of the
column chunks `bytemass` measured. The script prints both because they answer
different questions: how large the files are on disk, and how many of those
bytes belong to the columns.

The plumbum version needs one extra package:

```console no-run
$ pip install -r examples/python/requirements.txt
$ python3 examples/python/measure_files_plumbum.py examples/quickstart.parquet
files,rows,column_chunks,file_bytes,column_bytes
1,8,2,541,170
```

Both scripts accept several paths, `--fan-out` (files in flight, default 128),
and `--pqbench` (default `$PQB`, then `pqbench`).

## Why not `subprocess.run` per stage?

`subprocess.run` runs one command to completion and returns its output, so a
four-stage walk becomes four barriers. `Popen` lets the stages run at once and
passes bytes through the kernel pipe. The
[Python orchestration guide](../../docs/guides/python.md#which-pattern)
shows the patterns.
