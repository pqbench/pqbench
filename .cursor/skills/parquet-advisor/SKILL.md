---
name: parquet-advisor
description: >-
  Turns pqbench facts into write-path, table DDL, and compression-level
  recipes (with pros and cons). Use after bytemass, profile, or experiment
  when suggesting Parquet layout, sort keys, Iceberg/Delta DDL, ingest
  settings, codecs, or bytes-per-row improvements.
---

# Parquet advisor

The source of truth is bundled in the CLI. Read it before proposing
write or DDL changes:

```sh
pqbench skill parquet-advisor
pqbench skill parquet-advisor recipes
```

Follow that document: measure (bytemass → dump → profile → optional
locality → experiment), then emit recipes with evidence, write/ingest
settings, table DDL, compression level pros/cons, risk, and what is
unverified.
