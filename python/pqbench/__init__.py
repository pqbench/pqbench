"""Python bindings for the pqbench command line.

Each function runs one command inside this process and returns the same text
or JSON the CLI would print. ``pip install`` builds the native extension; a
separate ``pqbench`` binary is not required.

The walk is the verb-first surface: ``get_*`` reads one resource and
``list_*`` enumerates a collection, mirroring ``pqbench get …`` and
``pqbench list …``. The resources are the ``pqbench.v1`` wire types, so a value
returned here round-trips as JSON into ``bytemass``, ``list_files``, or any CLI
stage.
"""

from pqbench._native import (
    __version__,
    bytemass,
    commands,
    compression,
    credentials_check,
    credentials_get,
    diff,
    experiment,
    get_catalog,
    get_metastore,
    get_schema,
    get_table,
    list_catalogs,
    list_files,
    list_partitions,
    list_schemas,
    list_tables,
    lz,
    profile,
    ratelimit,
    setup,
    skill,
    viz,
)

__all__ = [
    "__version__",
    "bytemass",
    "commands",
    "compression",
    "credentials_check",
    "credentials_get",
    "diff",
    "experiment",
    "get_catalog",
    "get_metastore",
    "get_schema",
    "get_table",
    "list_catalogs",
    "list_files",
    "list_partitions",
    "list_schemas",
    "list_tables",
    "lz",
    "profile",
    "ratelimit",
    "setup",
    "skill",
    "viz",
]
