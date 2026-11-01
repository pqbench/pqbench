# Reference

The details, in the order you reach for them: the generated command reference,
then the documents, the configuration, the install methods, and the Python
wheel.

- [CLI](cli.md) — every command, its usage, arguments, options, and defaults.
  Generated from the clap command tree; do not edit by hand.
- [Documents](documents.md) — the records the commands exchange, and which
  command produces and consumes each one.
- [Profile](profile.md) — the per-column facts `profile` decodes from a row
  sample.
- [Diff](diff.md) — comparing two files or saved bytemass streams.
- [Visualization](viz.md) — folding a bytemass stream into `ColumnMass` rows
  and drawing them as a static HTML treemap.
- [Agent skills](skill.md) — the skills bundled into the binary.
- [Authentication](authentication.md) — object storage, catalog config, and
  Databricks-governed tables.
- [Configuration](configuration.md) — the `~/.pqbenchrc` profiles, the
  environment, and their precedence.
- [Distribution](distribution.md) — install the released binary, the supported
  targets, and how releases are cut.
- [Python](python.md) — the binary wheel, and how to read its files. For
  orchestration patterns, see the [Python guide](../guides/python.md).

`pqbench --help` is the long page; `pqbench <command> --help` is local to that
command. This section is the durable copy.
