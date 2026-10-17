"""The ``pqbench`` API: the client handles, and the walk.

The library's commands are the crate's modules, one function per path, under
``pqbench.native``::

    import pqbench.native

    pqbench.native.metastore.get.read(workspace)
    pqbench.native.catalog.ls.list(workspace)
    pqbench.native.file.ls.list(lake, uri, env, versions)
    pqbench.native.credentials.check.check(workspace, catalog, schema, table)

Every name there is the library function of the same path, with the same
arguments — a separate ``pqbench`` binary is not required. The records are the
library's serde types, marshalled as dicts; the commands whose library entry
point is async — the walk verbs, ``bytemass`` — return awaitables, and the
collections the library streams — ``partition.ls.list`` and ``file.ls.list`` —
are async iterators.

This package is the ergonomic surface over them: the client handles every
command takes, re-exported so they sit with ``walk``, the whole-metastore walk
composed once::

    workspace = pqbench.Workspace(pqbench.Config(endpoint, token))

    async for file in pqbench.walk(workspace):
        print(file["uri"])
"""

from pqbench import native
from pqbench.native import Config, Dialect, Lake, Layer, Workspace
from pqbench.walk import walk

__version__ = native.__version__

__all__ = [
    "__version__",
    "native",
    "walk",
    "Config",
    "Dialect",
    "Lake",
    "Layer",
    "Workspace",
]
