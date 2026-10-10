# What is a partition?

A partition is a **grouping of a table's active files by a key**. It is not a
thing the table stores; it is a lens on the files a snapshot already lists.

## The key is policy

Any attribute of a file can be the key, so "partition" means different things in
different tables:

- **Physical.** The Delta / Iceberg partition columns the log records
  (`partition_values`), e.g. `year=2024/month=09`. The table was written that
  way.
- **Natural.** Time — group files by when they entered the table:
  - Delta: the commit that added the file, and its `commitInfo.timestamp`.
  - Iceberg: the snapshot that added the file, and that snapshot's commit time.
- **Anything else.** Size band, path prefix, a value column's bounds. The key is
  whatever the reader needs.

Because the key is policy, a partition is a **view**, not an entity with a
lifecycle. The table's files are the truth; a partition is a projection of them.

## A decomposition, not a filter

Given a table state and a key, every file falls into exactly one group: the
decomposition is total and disjoint. Two consequences:

- **Unique per moment.** The same table state and key always yield the same
  partitions. A later commit can move a file to a different partition (a
  backfill) — that is the table changing, not the partition being ambiguous.
- **No gaps by construction.** A window with no files is simply not a group.
  Emitting empty windows is a separate, explicit choice.

The key must be **well defined**: for a natural partition, a half-open interval
`[first, last)` in UTC and a named time source. That is what makes two runs
agree.

## Where it sits

The resource hierarchy is `metastore → catalog → schema → table → partition →
file`. A partition is the unit of work below a table: small enough to measure on
its own, large enough to be worth a process.

- `table ls` groups a table's commits into **natural partitions** — one
  `pqbench.partition` per window, carrying the commits it holds.
- `partition ls` lists one partition's files — the input to `bytemass`.
- `partition info` describes one partition.
- `bytemass` measures one partition — an environment plus a list of files,
  nothing more.

A partition record carries its **definition** (the key: e.g. a natural
interval), the **env** to read its files, and the **files** themselves. Carrying
the env once per partition — never per file — is what keeps credentials cheap.

## Cost

Partitioning adds no memory of its own: it is a projection of the file metadata
the log read already produces. The costs that matter are:

- **Finding** a table's files pays the log-replay floor. For Delta the kernel
  records every file key before filtering, so peak memory is Ω(#files) however
  the files are later grouped.
- **Holding** a partition is O(partition), not O(#files): stream the files and
  drop each after it is emitted.
- **Measuring** a partition is O(partition): `bytemass` reads footers, one
  partition at a time.

So a partition can be read with memory proportional to the partition — provided
nothing retains the whole table's file set. That "nothing" is the hard rule: no
`collect()` on a log path.

## What it is not

- **Not physical-only.** A partition need not match a directory layout.
- **Not stored.** Nothing writes partitions back to the table.
- **Not a header.** A partition is a flat record (definition + env + files);
  there is no begin/end envelope carrying state across records.

See also: issue #58 (the target structure).
