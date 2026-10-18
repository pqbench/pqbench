"""The ``pqbench`` API: the client handles, the records, and the walk.

The library's commands are the crate's modules, one function per path, under
``pqbench.native``::

    import pqbench.native

    pqbench.native.metastore.get.get(workspace)
    pqbench.native.catalog.list.list(workspace)
    pqbench.native.file.list.list(lake, uri, env, versions)
    pqbench.native.credentials.check.check(workspace, catalog, schema, table)

Every name there is the library function of the same path, with the same
arguments — a separate ``pqbench`` binary is not required. The records are the
library's records, as classes — a field reads the same three ways:
``record["uri"]``, ``record.uri``, ``record.asdict()``; the commands whose entry
point is async — the walk verbs, ``bytemass`` — return awaitables, and the
collections the library streams — ``partition.list.list`` and ``file.list.list`` —
are async iterators.

This package is the ergonomic surface over them: the client handles every
command takes, re-exported so they sit with ``walk``, and the records every
command returns, re-exported so a return type has a name at this level too
(``pqbench.TableFile`` is ``pqbench.native.TableFile``). The whole-metastore
walk is composed once::

    workspace = pqbench.Workspace(pqbench.Config(endpoint, token))

    async for file in pqbench.walk(workspace):
        print(file["uri"])

Where the commands themselves are typed is ``pqbench/native.pyi`` — the stub
next to the extension module, which is what a caller's editor reads.
"""

from pqbench import native
from pqbench.native import (
    Catalog,
    Config,
    Dialect,
    Eligibility,
    Lake,
    Layer,
    MassRow,
    Partition,
    Record,
    Sample,
    Schema,
    Table,
    TableFile,
    TableRef,
    TypedSample,
    Workspace,
)
from pqbench.walk import walk

__version__ = native.__version__

__all__ = [
    "__version__",
    "native",
    "walk",
    # The client handles every command takes.
    "Config",
    "Dialect",
    "Lake",
    "Layer",
    "Workspace",
    # The records every command returns.
    "Record",
    "Catalog",
    "Eligibility",
    "MassRow",
    "Partition",
    "Sample",
    "Schema",
    "Table",
    "TableFile",
    "TableRef",
    "TypedSample",
]
