"""Python bindings for the pqbench command line.

Each function runs one command inside this process and returns the same text
or JSON the CLI would print. ``pip install`` builds the native extension; a
separate ``pqbench`` binary is not required.
"""

from pqbench import _native
from pqbench._native import __version__, commands

__all__ = ["__version__", "commands", *commands]

for _name in commands:
    globals()[_name] = getattr(_native, _name)

del _name
