# Recipes: write path, DDL, compression levels

Companion to `parquet-advisor`. Print with
`pqbench skill parquet-advisor recipes`.

## Compression levels (pros and cons)

Verify by sweeping the levels in the writer and comparing each output with
`pqbench bytemass` (bytes and bytes per value), on production-shaped data.

### zstd

| Level | Pros | Cons |
| --- | --- | --- |
| `1` | Fastest zstd write; still beats snappy on many tables | Weaker ratio than 3 |
| `3` | Default lake write; good bytes-per-value/CPU | Not the best ratio |
| `5`–`7` | Often a few percent better bytes per value on text/enums | Noticeably slower writes |
| `9` | Cold data, infrequent rewrite | High write CPU and memory |
| `15`–`22` | Last-resort archive | Diminishing returns; can stall ingest |

Use `zstd@3` unless a sweep shows `zstd@1` within ~5% bytes per value **and**
ingest is the bottleneck, or `zstd@7` wins >5% on the heavy
columns and writes are batch.

### Other codecs

| Codec | Pros | Cons |
| --- | --- | --- |
| `snappy` | Very fast encode/decode; common on Spark | Weaker ratio; poor on already-random bytes |
| `lz4` | Fast, similar niche to snappy | Weaker ratio; check reader support (`LZ4_RAW`) |
| `gzip` | Universal readers | Slow write and read |
| `uncompressed` | Zero CPU; correct when values are opaque blobs | Large files; only if `compression_ratio` is already ~1 |

If bytemass `compression_ratio` is already near 1, raising zstd level
will not help. Change type, dictionary, or sort first.

## Write / ingest recipes

Apply the chosen spec to the writer, not only to a sample.

### Sort and clustering

| Change | Ingest change |
| --- | --- |
| `sort:ts` | Sort the batch by `ts` before write. Spark: `repartition`/`sortWithinPartitions`. Iceberg: write sort-order. |
| `sort:country,city` | Composite sort; keep country first when `country` determines `city`. |
| `zorder:lat,lon` / `hilbert:lat,lon` | Engine Z-order/Hilbert if it has one; otherwise pre-order ranks in the job. Measure skip **and** bytes per value — Z-order can help skip and hurt size. |
| no sort win | Do not add a shuffle. |

### Dictionary, pages, row groups

| Setting | Ingest change |
| --- | --- |
| `dictionary:on` | Enable dict on low-NDV columns; leave off on unique ids / high-entropy blobs. |
| `dictionary:off` | Unique strings, UUID text, or dict pages that bloated the file. |
| `dictionary:BYTES` | Cap dict page (better page skip, more fallback PLAIN). |
| `page-size:8192` | Smaller pages: better predicate skip, more overhead (worse bytes per value). |
| `page-size:1048576` | Default-ish; better size, coarser skip. |
| `row-group-size:ROWS` | Smaller RGs: more skip granularity, more footer/RG overhead. |
| `encoding:delta` | Sorted ints/timestamps (integer columns). |
| `encoding:delta_byte_array` | Strings with long shared prefixes after sort. |
| `encoding:byte_stream_split` | Somewhat compressible floats (then zstd). |

Encodings are type-specific: apply a requested encoding only to compatible
columns.

### Representation (ingest-time, not a codec)

These are ingest decisions; confirm the current storage with `profile`
(`physical_kind`, `length_*`, `top_values`) and `bytemass` mass.

| Observation | Ingest change |
| --- | --- |
| UUID text in a `string` column | Write `FIXED_LEN_BYTE_ARRAY(16)` or two INT64s; never 36-char text. |
| Integer / decimal values stored as `string` | Parse to INT64 / DECIMAL at ingest; confirm with `bytemass`. |
| Timestamp text stored as `string` | Parse to TIMESTAMP (micros). |
| Very large `length_max` on a queried field | Extract queried fields to columns; leftover blob in its own file/column. |
| Boolean stored as `string` | Native BOOLEAN. |

## Table DDL recipes

No engine owns all knobs. Map the chosen settings to the table you actually write.

### Iceberg (Spark)

```sql
ALTER TABLE db.t WRITE ORDERED BY country, city, ts;
ALTER TABLE db.t SET TBLPROPERTIES (
  'write.parquet.compression-codec' = 'zstd',
  'write.parquet.compression-level' = '3',
  'write.parquet.page-size-bytes' = '8192',
  'write.parquet.row-group-size-bytes' = '134217728',
  'write.parquet.dict-size-bytes' = '2097152'
);
-- skipping aim: partition the filter column if it is low-NDV
ALTER TABLE db.t ADD PARTITION FIELD days(ts);
```

Iceberg sort-order is the durable form of `sort:A,B`. Partitioning is
for skip/planning, not a substitute for in-file sort.

### Delta (Spark)

```sql
ALTER TABLE db.t SET TBLPROPERTIES (
  'delta.parquet.compression.codec' = 'zstd'
);
-- clustering / ZORDER is the skipping recipe
OPTIMIZE db.t ZORDER BY (lat, lon);
```

Spark session write (when DDL cannot set level):

```python
(df.sortWithinPartitions("country", "city")
   .write.mode("append")
   .option("compression", "zstd")
   .saveAsTable("db.t"))
# spark.sql.parquet.compression.codec=zstd
# parquet.compression.codec.zstd.level=3
```

### Databricks Parquet v2

For Databricks Runtime 18.1 and later, opt future Delta writes into v2:

```sql
ALTER TABLE db.t SET TBLPROPERTIES (
  'delta.parquet.format.version' = '2.12.0'
);
```

Existing files are unchanged. On Runtime 18.2 and later, rewrite them with:

```sql
REORG TABLE db.t APPLY (SET PARQUET (FORMAT_VERSION = '2.12.0'));
```

Check reader compatibility before changing the table. Measure the actual
output: this selects an engine format policy, not a guarantee about any
individual column's encoding. The property value is distinct from the integer
version in a Parquet footer. See [Databricks Parquet v2](https://docs.databricks.com/aws/en/tables/features/parquet-v2).

### parquet-mr: column dictionaries and zstd levels

For a writer using parquet-mr's Hadoop configuration:

```java
conf.setBoolean("parquet.enable.dictionary", true);
conf.setBoolean("parquet.enable.dictionary#events.list.element", false);
conf.setInt("parquet.compression.codec.zstd.level", 7);
```

Replace the physical leaf path with the path in the output schema. The column
setting overrides the global dictionary default. Select zstd as the codec
separately; its level setting does not select it. These are Hadoop writer
properties, not universal Delta table properties. Verify that the actual
writer forwards them; do not assume Photon honors parquet-mr settings.
See [Apache parquet-java configuration](https://github.com/apache/parquet-java/blob/master/parquet-hadoop/README.md).

Databricks SQL warehouses accept only their documented configuration
parameters. The Hadoop zstd-level setting is not on that list, so do not use
an arbitrary SQL `SET` for it there. Use a writer with documented level
control and measure its output. See [Databricks SQL parameters](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-parameters).

### DuckDB

```sql
COPY t TO 'out.parquet' (
  FORMAT parquet,
  COMPRESSION zstd,
  COMPRESSION_LEVEL 3,
  ROW_GROUP_SIZE 122880
);
-- or CREATE TABLE … with ordered INSERT
INSERT INTO t SELECT * FROM src ORDER BY country, city;
```

### Generic writer properties

```text
compression          = zstd
compression_level    = 3          # from a writer sweep
dictionary_enabled   = true       # except unique/high-entropy cols
data_page_size       = 8192..1MiB # skip vs bytes per value
max_row_group_rows   = from a writer sweep
sorting_columns      = chosen sort
```

## Combining aims

- **Storage only:** pick the candidate with the lowest bytes / bytes per
  value. Put that codec/level and sort on the writer.
- **Skipping only:** pick the lowest skip span on the filter columns;
  accept a small size regression if point-skip improves.
- **Both:** prefer a candidate that improves skip on the filter column
  without raising bytes per value above ~1.05 versus the best storage
  candidate. If Z-order helps skip but loses >10% size, say so and let the
  owner choose.

## Recipe examples

### Verified sort + zstd@3

```markdown
## Recipe: sort country, city; zstd@3
- Aim: both
- Evidence: bytemass on the rewritten output 0.71× the source bytes;
  city skip span 0.12 vs 0.81 before (source: sample 8192 / footer)
- Write / ingest: sortWithinPartitions(country, city); zstd level 3
- Table DDL: Iceberg WRITE ORDERED BY country, city;
  write.parquet.compression-codec=zstd, compression-level=3
- Compression: zstd@3 — default lake tradeoff; @1 was +4% bytes,
  @7 was −2% bytes and ~2× write CPU on the sample
- Risk: shuffle cost at ingest; readers must honor file order
- Unverified: production file size after full-table rewrite
```


### Unverified type fix

```markdown
## Recipe: store device_id as bytes not uuid-string
- Aim: storage
- Evidence: profile physical_kind=string, length_mean 36 on device_id;
  bytemass device_id 13% of table (source: sample 8192 / footer)
- Write / ingest: parse UUID to 16 bytes at ingest
- Table DDL: BINARY(16) / UUID native type, not STRING
- Compression: leave zstd@3 — level will not shrink hex text much
- Risk: break text consumers
- Unverified: the writer change was not measured;
  measure the new output with bytemass
```
