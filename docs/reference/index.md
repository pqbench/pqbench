# Reference

The details, in the order you reach for them: the command table, then the
configuration, the install methods, and the per-command pages.

- [CLI](../cli.md) — the task-oriented command table, the document kinds that
  travel between commands, repeated flags, and auth for `s3://` and catalogs.
- [Configuration](../configuration.md) — the `~/.pqbenchrc` profiles, the
  environment, and their precedence.
- [Distribution](../distribution.md) — install the released binary, the
  supported targets, and how releases are cut.
- [`profile`](../profile.md) — per-column facts from a decoded row sample.
- [`experiment`](../experiment.md) — rewrite a sample and measure the result.
- [`diff`](../diff.md) — compare two Parquet files or saved bytemass streams,
  column by column.
- [`viz`](../viz.md) — collect a bytemass stream into a static HTML treemap.
- [Agent skills](../skill.md) — the skills compiled into `pqbench skill`.
- [Python bindings](../python.md) — the PyO3 wheel and its in-process API.

`pqbench --help` is the long page; `pqbench <command> --help` is local to that
command. This section is the durable copy.
