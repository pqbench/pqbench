# Python bindings

The `pqbench` wheel is a PyO3 binding: the commands live under `pqbench.native`,
one per library entry point, and `pqbench` adds the client handles they take and
`walk`, the whole-metastore walk — so measurement fits inside a notebook or a
pipeline. The text below is included from the source
[`python/README.md`](https://github.com/pqbench/pqbench/blob/main/python/README.md),
so it cannot drift from the wheel.

<!-- The include starts at python/README.md's `## Install`; update the line if
the headings above it move. -->

--8<-- "python/README.md:13:"

## Typing

The wheel ships `pqbench/native.pyi` beside the extension module, so a caller's
editor and type checker see the commands: which verbs are awaitable, which
collections stream, and a record's fields by name. `python/tests/test_native_stub.py`
holds the stub and the module in step — every name, and every parameter — so a
command cannot be added without its type.
