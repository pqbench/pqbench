# Agent skills

`pqbench skill` prints a skill that is compiled into the binary. An
agent loads it the same way it loads `--help`: run the command, read
the markdown.

```sh
pqbench skill
pqbench skill parquet-advisor
pqbench skill parquet-advisor recipes
```

No `-o` on a TTY. A list is one `pqbench.skill` JSON line per skill.
A named skill is raw markdown.

## parquet-advisor

Turns **facts** (bytemass, profile, experiment) into **recipes**:

- write / ingest settings (sort, dictionary, page size, casts)
- table DDL (Iceberg sort-order, Delta ZORDER, Spark options, DuckDB COPY)
- compression codec and **level**, with pros and cons

It does not measure files and does not replace `experiment`. The
workflow is: L0 byte mass → dump → profile → optional locality →
bounded rewrites → recipe with evidence.

Project copy for Cursor: `.cursor/skills/parquet-advisor/`. Source
markdown: `skills/parquet-advisor/`.
