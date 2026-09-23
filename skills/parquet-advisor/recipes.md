# Recipes: write path, DDL, compression levels

Companion to `parquet-advisor`. Print with
`pqbench skill parquet-advisor recipes`.

## Compression levels (pros and cons)

Verify with
`pqbench experiment sample.parquet --rewrite codec:zstd@1 --rewrite codec:zstd@3 --rewrite codec:zstd@7 --rewrite codec:snappy --aim storage`.

### zstd

| Level | Pros | Cons |
| --- | --- | --- |
| `1` | Fastest zstd write; still beats snappy on many tables | Weaker ratio than 3 |
| `3` | Default in `dump` / experiment control; good BPR/CPU | Not the best ratio |
| `5`–`7` | Often a few percent better BPR on text/enums | Noticeably slower writes |
| `9` | Cold data, infrequent rewrite | High write CPU and memory |
| `15`–`22` | Last-resort archive | Diminishing BPR; can stall ingest |

Use `zstd@3` unless experiment shows `zstd@1` within ~5% BPR **and**
ingest is the bottleneck, or `zstd@7` wins >5% BPR on the heavy
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

Apply the **winning** experiment spec to the writer, not only to the
sample.

### Sort and clustering

| Experiment winner | Ingest change |
| --- | --- |
| `sort:ts` | Sort the batch by `ts` before write. Spark: `repartition`/`sortWithinPartitions`. Iceberg: write sort-order. |
| `sort:country,city` | Composite sort; keep country first when FD(country→city) is high. |
| `zorder:lat,lon` / `hilbert:lat,lon` | Engine Z-order/Hilbert if it has one; otherwise pre-order ranks in the job. Measure skip **and** BPR — Z-order can help skip and hurt BPR. |
| no sort win | Do not add a shuffle. |

### Dictionary, pages, row groups

| Experiment winner | Ingest change |
| --- | --- |
| `dictionary:on` | Enable dict on low-NDV columns; leave off on unique ids / high-entropy blobs. |
| `dictionary:off` | Unique strings, UUID text, or dict pages that bloated the file. |
| `dictionary:BYTES` | Cap dict page (better page skip, more fallback PLAIN). |
| `page-size:8192` | Smaller pages: better predicate skip, more overhead (worse BPR). |
| `page-size:1048576` | Default-ish; better BPR, coarser skip. |
| `row-group-size:ROWS` | Smaller RGs: more skip granularity, more footer/RG overhead. |
| `encoding:delta` | Sorted ints/timestamps. |
| `encoding:delta_byte_array` | Strings with long shared prefixes after sort. |
| `encoding:byte_stream_split` | Somewhat compressible floats (then zstd). |

### Representation (ingest-time, not a codec)

| Finding | Ingest change |
| --- | --- |
| `uuid` as STRING | Write `FIXED_LEN_BYTE_ARRAY(16)` or two INT64s; never 36-char text. |
| `integer_string` / `decimal_string` | Parse to INT64 / DECIMAL at ingest. |
| `timestamp_string` | Parse to TIMESTAMP (micros). |
| `json` / huge strings | Extract queried fields to columns; leftover blob in its own file/column. |
| boolean-as-string | Native BOOLEAN. |

## Table DDL recipes

No engine owns all knobs. Map the winner to the table you actually write.

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
compression_level    = 3          # from experiment
dictionary_enabled   = true       # except unique/high-entropy cols
data_page_size       = 8192..1MiB # skip vs BPR
max_row_group_rows   = from experiment
sorting_columns      = winning sort:
```

## Combining aims

- **Storage only:** pick the trial with lowest `file_bytes` /
  `bytes_per_row`. Put that codec/level and sort on the writer.
- **Skipping only:** pick lowest `skip_span_ratio` on the filter
  columns; accept a small BPR regression if point-skip improves.
- **Both:** prefer a trial that improves skip on the filter column
  without raising `file_bytes_ratio` above ~1.05 versus the best
  storage trial. If Z-order helps skip but loses >10% BPR, say so
  and let the owner choose.

## Recipe examples

### Verified sort + zstd@3

```markdown
## Recipe: sort country, city; zstd@3
- Aim: both
- Evidence: experiment `sort:country,city` file_bytes_ratio 0.71;
  city skip_span_ratio 0.12 vs control 0.81 (sample 8192)
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
- Evidence: profile pattern=uuid, bytemass device_id 13% of table
  (source: sample 8192 / footer)
- Write / ingest: parse UUID to 16 bytes at ingest
- Table DDL: BINARY(16) / UUID native type, not STRING
- Compression: leave zstd@3 — level will not shrink hex text much
- Risk: break text consumers
- Unverified: experiment cast is int64-only today; measure after
  the writer change
```
