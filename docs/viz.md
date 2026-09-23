# Visualize a bytemass stream

`bytemass` measures Parquet footers and writes NDJSON. `pqbench viz` collects
those lines into SQLite and a static HTML page. Measurement and presentation
are separate commands so either end can change without the other.

```sh
pqbench bytemass data.parquet | pqbench viz -o report
pqbench table ./delta-table | pqbench bytemass | pqbench viz -o report
pqbench lake ./warehouse | pqbench table | pqbench bytemass | pqbench viz -o report
xdg-open report.html
```

`-o report` writes `report.sqlite` and `report.html`. A terminal prints a
short summary. The HTML file embeds the database; open it without a server.

```mermaid
flowchart LR
    files[Parquet / table / lake] --> bytemass[pqbench bytemass]
    bytemass --> stream["pqbench.bytemass-file + row lines"]
    stream --> viz[pqbench viz]
    viz --> sqlite[report.sqlite]
    viz --> html[report.html]
```

## Stream

`bytemass` first writes the table-file stats it received:

```json
{"kind":"pqbench.bytemass-file","id":"sales/orders","path":"year=2024/part-0.parquet","file":"s3://…/part-0.parquet","size":1200,"stats":{"num_records":3000,"bytes_per_row":0.4}}
```

Each measured column is then one line:

```json
{"kind":"pqbench.bytemass-row","id":"sales/orders","file":"part-0.parquet","size":1200,"num_rows":3000,"column":"text","compressed_bytes":80,"uncompressed_bytes":240,"codec":"ZSTD"}
```

`id` is the table the file belongs to, or empty for a bare parquet path.
`viz` does not reread the files.

## SQLite

`masses` is one row per column chunk:

| column | meaning |
| --- | --- |
| `id` | table id from the stream |
| `file` | input path or URI |
| `size` | on-disk file size |
| `num_rows` | rows in the file |
| `column_path` | schema path (`text`, `a.b`) |
| `compressed_bytes` | on-disk bytes for the chunk |
| `uncompressed_bytes` | encoded bytes before compression |
| `codec` | codec recorded in the footer |
| `encodings` | encodings listed on the chunk |
| `num_values` | values in the chunk (including nulls) |
| `dictionary` | 1 when a dictionary page offset is present |
| `null_count` / `distinct_count` | footer statistics, when present |
| `physical_type` | leaf physical type |
| `row_group` / `row_group_rows` | chunk's row group |
| `compressed_bytes_per_row` | compressed bytes / row-group rows |
| `page_count` | OffsetIndex pages, when `bytemass --indexes` |

`files` is one row per proxied table file (empty when the stream had only
bare parquet paths):

| column | meaning |
| --- | --- |
| `id` | table id from the stream |
| `path` | table-relative path |
| `file` | URI or filesystem path |
| `size` | log or object size |
| `num_records` | Delta `numRecords`, when present |
| `bytes_per_row` | log size / num_records |
| `storage_class` | object tier from HEAD, when known |
| `partition` | hive `k=v/k=v` |

```sh
sqlite3 report.sqlite 'SELECT column_path, SUM(compressed_bytes) FROM masses GROUP BY column_path'
sqlite3 report.sqlite 'SELECT partition, SUM(size), SUM(num_records) FROM files GROUP BY partition'
```

## HTML

The page loads [sql.js](https://sql.js.org) and the d3 modules it uses
(hierarchy, scale, selection) from a CDN. JavaScript opens the embedded
SQLite file, groups rows by `id`, and draws a column treemap of on-disk
bytes per row plus a file/partition treemap when `files` is populated.
Several tables become a list plus one pair of maps each. No build step, no
npm, no local server.
