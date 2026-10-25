# Showcases

pqbench earns its keep on real files, not on a two-column smoke sample. Each
showcase below is a job you might actually run — find the columns that own the
bytes, choose a codec, verify a layout change, read a file from the wild — with
the numbers the tool printed.

The first three run on the committed Reddit comments sample
(`crates/pqbench-cli/tests/fixtures/small_reddit_none.parquet`, 3000 rows, 7
columns). The last two use larger public datasets committed under
`data/samples/` (Git LFS). All five blocks run in the gate; after a clone that
skipped LFS, fetch and verify the samples first:

```console no-run
$ git lfs pull
$ make samples
```

## Where the bytes go

A 2.1 MB Reddit sample. Before tuning anything, find the columns that own the
bytes — `bytemass` reads only the footer, so it answers in milliseconds:

```console run
$ pqbench table-file crates/pqbench-cli/tests/fixtures/small_reddit_none.parquet | pqbench bytemass | pqbench format
column            codec         encodings                  bytes  values
----------------  ------------  ------------------------  ------  ------
text              UNCOMPRESSED  PLAIN,RLE,RLE_DICTIONARY  743566    3000
label             UNCOMPRESSED  PLAIN,RLE,RLE_DICTIONARY    2469    3000
dataType          UNCOMPRESSED  PLAIN,RLE,RLE_DICTIONARY     360    3000
communityName     UNCOMPRESSED  PLAIN,RLE,RLE_DICTIONARY    2468    3000
datetime          UNCOMPRESSED  PLAIN,RLE,RLE_DICTIONARY      87    3000
username_encoded  UNCOMPRESSED  PLAIN,RLE,RLE_DICTIONARY  446197    3000
url_encoded       UNCOMPRESSED  PLAIN,RLE,RLE_DICTIONARY  907117    3000
```

Three columns — `url_encoded` (907 KB), `text` (744 KB), `username_encoded`
(446 KB) — are 99.7% of the 2.10 MB file. The other four together are about
5 KB. That is the whole triage: four columns are noise, three are the file.

`profile` decodes a row sample to explain *why*:

```console run
$ pqbench profile crates/pqbench-cli/tests/fixtures/small_reddit_none.parquet | pqbench format
id                                                           column            kind    values  nulls   ndv
-----------------------------------------------------------  ----------------  ------  ------  -----  ----
crates/pqbench-cli/tests/fixtures/small_reddit_none.parquet  text              string    3000      0  2753
crates/pqbench-cli/tests/fixtures/small_reddit_none.parquet  label             string    3000      0    32
crates/pqbench-cli/tests/fixtures/small_reddit_none.parquet  dataType          string    3000      0     2
crates/pqbench-cli/tests/fixtures/small_reddit_none.parquet  communityName     string    3000      0    32
crates/pqbench-cli/tests/fixtures/small_reddit_none.parquet  datetime          string    3000      0     1
crates/pqbench-cli/tests/fixtures/small_reddit_none.parquet  username_encoded  string    3000      0  3000
crates/pqbench-cli/tests/fixtures/small_reddit_none.parquet  url_encoded       string    3000      0  3000
```

`url_encoded` and `username_encoded` are unique on every row (NDV 3000) — they
are hashed ids stored as text, so they compress poorly and cost the most per
row. `dataType` (2 values) and `datetime` (1 value) are dictionary-perfect and
free. Optimize the three big columns; leave the rest alone.

The same stream draws a treemap, with no server and no build step:

```console run
$ pqbench table-file crates/pqbench-cli/tests/fixtures/small_reddit_none.parquet | pqbench bytemass | pqbench viz -o /tmp/reddit
```

## Which codec

The sample is stored **uncompressed**, so the codec is the first lever. Sweep
the codecs over the file's encoded pages:

```console run
$ pqbench compression crates/pqbench-cli/tests/fixtures/small_reddit_none.parquet --samples 1 --warmup-iterations 0 | pqbench format
codec   level  compress MB/s  decompress MB/s  ratio
------  -----  -------------  ---------------  -----
zstd        1           ±458            ±1722   0.56
gzip        1            ±68             ±270   0.58
lz4         1           ±897            ±8476   0.81
snappy      1           ±912            ±2101   0.81
```

Speeds vary run to run; the ratios do not. zstd takes the file to 0.56 of its
bytes (44% smaller); snappy and lz4 reach only 0.81 (19% smaller) but compress
about twice as fast. gzip buys nothing over zstd and writes several times
slower. Unless write or decode speed is the binding constraint, zstd is the
answer here.

## Files from the wild

The committed samples in `data/samples/` are real Parquet from public datasets:
SQuAD (nested lists) and the TPC-DS `customer` table (`DELTA_BYTE_ARRAY`
encoding). `bytemass` folds each column's row groups into one record, so the
stream is already a per-column total:

```console run
$ pqbench table-file data/samples/squad.parquet \
>     | pqbench bytemass \
>     | jq -r 'select(.name | endswith("/bytemass")) | [(.name | capture("/columns/(?<c>[^/]+)/bytemass$").c), .compressed_bytes] | @tsv' \
>     | sort -k2 -nr
context                             8490172
question                            3027096
answers.text.list.item              1515148
id                                   709562
answers.answer_start.list.item       304136
title                                 18488
```

`context` alone is 8.5 MB of the 14.5 MB file (59%); `context` and `question`
together are 80%. A nested list column shows up as a dotted **physical** path
(`answers.text.list.item`), and `diff --depth` sums such prefixes when you
compare two layouts.

A real table with a non-default encoding reads the same way:

```console run
$ pqbench table-file data/samples/delta_byte_array.parquet | pqbench bytemass | pqbench format
column                 codec         encodings         bytes  values
---------------------  ------------  ----------------  -----  ------
c_customer_id          UNCOMPRESSED  DELTA_BYTE_ARRAY   8248    1000
c_salutation           UNCOMPRESSED  DELTA_BYTE_ARRAY   3362    1000
c_first_name           UNCOMPRESSED  DELTA_BYTE_ARRAY   6595    1000
c_last_name            UNCOMPRESSED  DELTA_BYTE_ARRAY   6955    1000
c_preferred_cust_flag  UNCOMPRESSED  DELTA_BYTE_ARRAY   1220    1000
c_birth_country        UNCOMPRESSED  DELTA_BYTE_ARRAY   9599    1000
c_login                UNCOMPRESSED  DELTA_BYTE_ARRAY     42    1000
c_email_address        UNCOMPRESSED  DELTA_BYTE_ARRAY  27763    1000
c_last_review_date     UNCOMPRESSED  DELTA_BYTE_ARRAY   3511    1000
```

The footer reports the encoding the writer actually used, and
`c_email_address` (27 KB) dominates a table whose every other column is under
10 KB — the same triage, on a file from a different ecosystem.

## Where to go next

- **Every command** — [CLI reference](cli.md) has the task table, documents, and
  auth for `s3://` and catalogs.
- **The same pipe over Delta, Iceberg, and a catalog** — [Visual demos](demo.md).
- **Recipes from the facts** — `pqbench skill parquet-advisor`
  ([Agent skills](skill.md)) turns a `bytemass` or `profile` result into a
  write-path or DDL change.
