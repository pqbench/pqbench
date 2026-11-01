# Showcases

pqbench earns its keep on real files, not on a two-column smoke sample. Each
showcase below is a job you might actually run — find the columns that own the
bytes, choose a codec, verify a layout change, read a file from the wild — with
the numbers the tool printed.

The first four run on the committed Reddit comments sample
(`crates/pqbench-cli/tests/fixtures/small_reddit_none.parquet`, 3000 rows, 7
columns). The last two use larger public datasets committed under
`data/samples/` (Git LFS). All six blocks run in the gate; after a clone that
skipped LFS, fetch and verify the samples first:

```console no-run
$ git lfs pull
$ make samples
```

## Where the bytes go

A 2.1 MB Reddit sample. Before tuning anything, find the columns that own the
bytes — `bytemass` reads only the footer, so it answers in milliseconds:

```console run
$ pqb table-file crates/pqbench-cli/tests/fixtures/small_reddit_none.parquet | pqb bytemass | pqb fmt
column            type        codec         encoding   bytes  dict bytes  bytes/row  values
----------------  ----------  ------------  --------  ------  ----------  ---------  ------
text              BYTE_ARRAY  UNCOMPRESSED  dict      743566      738658     247.86    3000
label             BYTE_ARRAY  UNCOMPRESSED  dict        2469         527       0.82    3000
dataType          BYTE_ARRAY  UNCOMPRESSED  dict         360          33       0.12    3000
communityName     BYTE_ARRAY  UNCOMPRESSED  dict        2468         527       0.82    3000
datetime          BYTE_ARRAY  UNCOMPRESSED  dict          87          28       0.03    3000
username_encoded  BYTE_ARRAY  UNCOMPRESSED  dict      446197      441331     148.73    3000
url_encoded       BYTE_ARRAY  UNCOMPRESSED  dict      907117      901963     302.37    3000
```

Three columns — `url_encoded` (907 KB), `text` (744 KB), `username_encoded`
(446 KB) — are 99.7% of the 2.10 MB file. The other four together are about
5 KB. That is the whole triage: four columns are noise, three are the file.

`profile` decodes a row sample to explain *why*:

```console run
$ pqb profile crates/pqbench-cli/tests/fixtures/small_reddit_none.parquet | pqb fmt
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
$ pqb table-file crates/pqbench-cli/tests/fixtures/small_reddit_none.parquet | pqb list columns | pqb summarize columns | pqb format --html > /tmp/reddit.html
```

## Which codec

The sample is stored **uncompressed**, so the codec is the first lever. Sweep
the codecs over the file's encoded pages:

```console run
$ pqb compression crates/pqbench-cli/tests/fixtures/small_reddit_none.parquet --samples 1 --warmup-iterations 0 | pqb fmt
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
$ pqb table-file data/samples/squad.parquet \
>     | pqb bytemass \
>     | jq -r 'select(.mass) | [(.name | capture("/columns/(?<c>[^/]+)$").c), .mass.compressed_bytes] | @tsv' \
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
$ pqb table-file data/samples/delta_byte_array.parquet | pqb bytemass | pqb fmt
column                 type        codec         encoding          bytes  dict bytes  bytes/row  values
---------------------  ----------  ------------  ----------------  -----  ----------  ---------  ------
c_customer_id          BYTE_ARRAY  UNCOMPRESSED  DELTA_BYTE_ARRAY   8248           0       8.25    1000
c_salutation           BYTE_ARRAY  UNCOMPRESSED  DELTA_BYTE_ARRAY   3362           0       3.36    1000
c_first_name           BYTE_ARRAY  UNCOMPRESSED  DELTA_BYTE_ARRAY   6595           0       6.59    1000
c_last_name            BYTE_ARRAY  UNCOMPRESSED  DELTA_BYTE_ARRAY   6955           0       6.96    1000
c_preferred_cust_flag  BYTE_ARRAY  UNCOMPRESSED  DELTA_BYTE_ARRAY   1220           0       1.22    1000
c_birth_country        BYTE_ARRAY  UNCOMPRESSED  DELTA_BYTE_ARRAY   9599           0       9.60    1000
c_login                BYTE_ARRAY  UNCOMPRESSED  DELTA_BYTE_ARRAY     42           0       0.04    1000
c_email_address        BYTE_ARRAY  UNCOMPRESSED  DELTA_BYTE_ARRAY  27763           0      27.76    1000
c_last_review_date     BYTE_ARRAY  UNCOMPRESSED  DELTA_BYTE_ARRAY   3511           0       3.51    1000
```

The footer reports the encoding the writer actually used, and
`c_email_address` (27 KB) dominates a table whose every other column is under
10 KB — the same triage, on a file from a different ecosystem.

## Where to go next

- **Every command** — the [CLI reference](../reference/cli.md) documents every
  command, its flags, and its output.
- **The same pipe over Delta, Iceberg, and a catalog** — [Visual demos](demo.md).
- **Recipes from the facts** — `pqb skill parquet-advisor`
  ([Agent skills](../reference/cli.md#pqbench-skill)) turns a `bytemass` or `profile` result into a
  write-path or DDL change.
