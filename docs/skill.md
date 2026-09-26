# Agent skills

`pqbench skill` prints a skill that is compiled into the binary. An
agent loads it the same way it loads `--help`: run the command, read
the markdown.

`pqbench skill` with no argument lists the bundled skills as JSON lines (this
is the command's explicit machine output). A named skill prints raw markdown:

```console run json
$ pqbench skill
{"kind":"pqbench.skill","version":1,"name":"parquet-advisor","description":"Turns pqbench facts into write-path, table DDL, and compression-level recipes","documents":["parquet-advisor","recipes"]}
```

```console run
$ pqbench skill parquet-advisor | head -1
---
$ pqbench skill parquet-advisor recipes | head -1
# Recipes: write path, DDL, compression levels
```

No `-o` on a TTY. A named skill is raw markdown.

## parquet-advisor

Turns **facts** (bytemass, profile, experiment) into **recipes**:

- write / ingest settings (sort, dictionary, page size, casts)
- table DDL (Iceberg sort-order, Delta ZORDER, Spark options, DuckDB COPY)
- compression codec and **level**, with pros and cons

It does not measure files and does not replace `experiment`. The
workflow is: L0 byte mass → profile → bounded rewrites → recipe with
evidence.

Project copy for Cursor: `.cursor/skills/parquet-advisor/`. Source
markdown: `skills/parquet-advisor/`.
