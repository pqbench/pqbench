"""Python bindings for the pqbench command line.

Each function runs one command inside this process and returns the same text
or JSON the CLI would print. ``pip install`` builds the native extension; a
separate ``pqbench`` binary is not required.
"""

from pqbench._native import (
    __version__,
    bytemass,
    commands,
    compression,
    dump,
    lake,
    lz,
    table,
    viz,
)

__all__ = [
    "__version__",
    "bytemass",
    "commands",
    "compression",
    "dump",
    "lake",
    "lz",
    "table",
    "viz",
]
