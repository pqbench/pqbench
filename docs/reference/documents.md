# Documents

The commands exchange typed JSON documents. A resource is one line, identified
by the collection segment of its `name`; a value type is nested inside the
resource that carries it; a document is a command's own output when it has no
addressable name. [Data modelling](../concepts/data-modelling.md) is the
contract; [`v1.proto`](../concepts/v1.proto) is the field-by-field resource
schema.

## Resource records

| Record | Name | Produced by | Consumed by |
| --- | --- | --- | --- |
| `Metastore` | `metastores/{metastore}` | [`get metastore`](cli.md#pqbench-get-metastore) | humans / scripts |
| `Catalog` | `catalogs/{catalog}` | [`list catalogs`](cli.md#pqbench-list-catalogs), [`get catalog`](cli.md#pqbench-get-catalog) | `get catalog`, `list schemas` |
| `Schema` | `catalogs/{catalog}/schemas/{schema}` | [`list schemas`](cli.md#pqbench-list-schemas), [`get schema`](cli.md#pqbench-get-schema) | `get schema`, `list tables` |
| `Table` | `catalogs/{catalog}/schemas/{schema}/tables/{table}` | [`list tables`](cli.md#pqbench-list-tables), [`get table`](cli.md#pqbench-get-table), [`credentials get`](cli.md#pqbench-credentials) | `get table`, `list files`, `credentials get` |
| `TableFile` | `…/tables/{table}/files/{file}` | [`list files`](cli.md#pqbench-list-files), [`sample files`](cli.md#pqbench-sample-files), [`table-file`](cli.md#pqbench-table-file) | `sample files`, `bytemass` |
| `Blob` | `blobs/{service}/{locator}` | [`bytemass`](cli.md#pqbench-bytemass) | `diff` |
| `Column` | `blobs/{service}/{locator}/columns/{column}` | [`bytemass`](cli.md#pqbench-bytemass) | `summarize columns`, `diff` |
| `ColumnProfile` | `blobs/{service}/{locator}/columns/{column}/profile` | [`profile`](profile.md) | humans / scripts |

`profile` adds the CLI's `id` (the input path) to each `ColumnProfile` row;
see [Profile](profile.md). The `Bytemass` value is nested in `Column.mass`; it
has no resource name of its own.

A resource's `name` is its identity. The collection segment (`catalogs`,
`schemas`, `tables`, `files`, `blobs`, `columns`) names the type, so no resource
carries a `kind` or a version of its own — the API version is the package's
(`pqbench.v1`). A dotted FQN such as `dbx_samples.nyctaxi.trips` is an
input-only lookup alias; the CLI translates it to the canonical path name.

## Command documents

Some commands emit a document that is not a resource: it has no addressable
`name`, or it is a listing that is meant to be read rather than piped.

| Document | Produced by | Shape |
| --- | --- | --- |
| `ReportRow` | [`lz`](cli.md#pqbench-lz), [`compression`](cli.md#pqbench-compression) | `name`, `codec`, `level`, `compress_estimate`, `decompress_estimate`, `compressed_bytes`, `uncompressed_bytes`, `ratio` |
| `ColumnRow` | [`compression --per-column`](cli.md#pqbench-compression) | `ReportRow` fields plus `column` |
| `ColumnDelta` | [`diff`](diff.md) | `column`, `left_bytes`, `right_bytes`, `delta_bytes`, `change_percent`, `left_values`, `right_values`, `left_bytes_per_value`, `right_bytes_per_value` |
| `ColumnMass` | [`summarize columns`](cli.md#pqbench-summarize-columns) | `table`, `column`, `compressed_bytes`, `uncompressed_bytes`, `value_count`, `bytes_per_value`, `compression_ratio`, `file_count`, `storage_classes`, `storage_costs` |
| `Skill` | [`skill`](skill.md) (no arguments) | `kind: "pqbench.skill"`, `version`, `name`, `description`, `documents` |

`ColumnDelta` has no `name`, so `pqb fmt` cannot place it in a table; pipe it
to `jq` or read the JSON. A `ColumnMass` row has no name either, but its shape
is recognized, so `pqb fmt` renders it as a table and `format --html` draws it.
`Skill` is the one document that keeps an explicit `kind` and `version`: it is
a listing, not a resource stream.

## Sampling

`sample files` keeps a rate of the `TableFile` stream, by size, so a table's
footers can be measured without reading every file:

```console run delta
$ cat docker/e2e-lakehouse/table.ndjson | pqb list files | pqb sample files --rate 1 2>/dev/null | pqb bytemass | pqb fmt
column  type        codec   encoding  bytes  dict bytes  bytes/row  values
------  ----------  ------  --------  -----  ----------  ---------  ------
id      INT64       SNAPPY  dict         66          37      22.00       3
label   BYTE_ARRAY  SNAPPY  dict         72          43      24.00       3
```

`--rate 1` keeps every file; the stage's coverage line is on standard error, so
the pipe above hides it to show the table. A file is kept with probability
`rate × size / 256 MiB`, capped at one, drawn from a hash of its name: the
same record always makes the same decision. A 256 MiB file is kept at
`--rate` (the default keeps about one in a hundred), a 1 GiB file four times
as often. The decision
needs only the record's name and size, which the log already carries, so the
stage streams and never holds the table.

Each kept `TableFile` records the probability it was kept with as
`sample_rate`; `bytemass` copies it, with the file's `size_bytes` and
`storage_class`, onto every `Column` (the `Blob` carries it too), so a column
prices its own storage. Scale
only the additive quantities by its inverse — `size_bytes`, `compressed_bytes`,
`uncompressed_bytes`, `dictionary_bytes`, `unencoded_bytes`, `value_count`,
`null_count`, `row_group_count` — and recompute the ratios (`bytes_per_value`,
`bytes_per_row`, `compression_ratio`, `null_fraction`, `dictionary_fraction`)
from the weighted sums; `distinct_count` is not additive. A sum over the kept
records alone is the sample, not the table — the coverage line on standard
error says how many files and bytes it holds. A record that already carries a
`sample_rate` composes with the smaller of the two probabilities, since both
threshold the same hash. `summarize columns` applies the weight, so a sampled
stream's rows estimate the table: the bytes, value counts, file count, and
monthly cost are each scaled by the inverse of the rate, and `format --html`
draws those estimates. A record with no positive size is an error, not a
silent drop.

Filtering by path or partition is still a shell job on the `list files` stream
(`jq`, `sort`, `head`, `xargs`) — see [Visual demos](../guides/demo.md).
`bytemass` reads its files in flight (`--fan-out`, 128 by default), so the
per-file footer round trips overlap instead of adding up.
